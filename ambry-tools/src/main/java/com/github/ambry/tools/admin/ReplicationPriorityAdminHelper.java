/**
 * Copyright 2026 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.tools.admin;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.network.ResponseInfo;
import com.github.ambry.network.SendWithCorrelationId;
import com.github.ambry.protocol.AdminRequest;
import com.github.ambry.protocol.AdminRequestOrResponseType;
import com.github.ambry.protocol.AdminResponse;
import com.github.ambry.protocol.ListReplicationPriorityAdminRequest;
import com.github.ambry.protocol.ListReplicationPriorityAdminResponse;
import com.github.ambry.protocol.UpdateReplicationPriorityAdminRequest;
import com.github.ambry.replication.PriorityEntry;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.utils.NettyByteBufDataInputStream;
import com.github.ambry.utils.Utils;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Performs the {@code SetReplicationPriority} and {@code ListReplicationPriority} admin operations on behalf of
 * {@link ServerAdminTool}.
 *
 * <p>{@code SetReplicationPriority} marks a set of partitions as replication priorities — or clears them — so the
 * receiving servers bias their replication scheduler toward those partitions until they catch up.
 * {@code ListReplicationPriority} reports the priorities currently in effect. Both operations are scoped to a
 * single fabric and fan out to the servers in that fabric that host the partitions (optionally narrowed to
 * specific servers); a set/clear sends each host only the partitions it actually holds.
 *
 * <p>This class owns no network client: it resolves targets from the {@link ClusterMap}, builds the wire requests,
 * and delegates the send to {@link ServerAdminTool} via the {@link Sender} interface. Per-host failures are
 * isolated and reported rather than aborting the operation, and each operation's result is written as JSON to
 * stdout.
 */
public class ReplicationPriorityAdminHelper {
  private static final String CLIENT_ID = "ServerAdminTool";
  private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationPriorityAdminHelper.class);

  private final ClusterMap clusterMap;
  private final Sender sender;
  private final AtomicInteger correlationId = new AtomicInteger(0);

  /**
   * The transport interface this helper uses to send a built request to a single host and receive the raw response.
   * Implemented in production by {@code ServerAdminTool#sendRequestGetResponse}; the signature matches it exactly
   * so a method reference satisfies it.
   */
  @FunctionalInterface
  public interface Sender {
    /**
     * Sends {@code request} to {@code node} and returns the raw response.
     * @param node the target {@link DataNodeId}.
     * @param partitionForReplicaResolution the partition used only to pick a replica on the node; may be
     *                                       {@code null} for partition-agnostic requests.
     * @param request the request to send.
     * @return the {@link ResponseInfo}.
     * @throws TimeoutException if the response was not received in time.
     */
    ResponseInfo send(DataNodeId node, PartitionId partitionForReplicaResolution, SendWithCorrelationId request)
        throws TimeoutException;
  }

  /**
   * The per-host unit of work for {@link #fanOut}. A thrown checked exception is isolated to that host (counted as
   * that host's failure) and does not abort the others. Both the set/clear and list paths supply a {@code work}
   * lambda; the set path's lambda turns a non-{@code NoError} {@link ServerErrorCode} into an
   * {@link IOException} so error-code and transport failures funnel through the same isolation.
   */
  @VisibleForTesting
  @FunctionalInterface
  interface HostWork {
    /**
     * Performs the per-host work.
     * @param host the target host.
     * @throws IOException on a transport/parse/server-error failure (isolated per host by {@link #fanOut}).
     * @throws TimeoutException if the host did not respond in time (isolated per host by {@link #fanOut}).
     */
    void run(DataNodeId host) throws IOException, TimeoutException;
  }

  /**
   * @param clusterMap the {@link ClusterMap} to resolve fabrics/hosts/partitions against.
   * @param sender the transport interface used to send each built request.
   */
  public ReplicationPriorityAdminHelper(ClusterMap clusterMap, Sender sender) {
    this.clusterMap = clusterMap;
    this.sender = sender;
  }

  /**
   * Derives the {@link UpdateReplicationPriorityAdminRequest.Action} from the {@code clear} flag and the
   * partition list, validating the partition-list shape and {@code boost} against the same rules the
   * server enforces. Pure function — no I/O — so every branch is unit-testable.
   *
   * <ul>
   *   <li>{@code clear=false}, non-empty partitions → {@link UpdateReplicationPriorityAdminRequest.Action#SET}
   *       (empty partitions rejected).</li>
   *   <li>{@code clear=true}, non-empty partitions → {@link UpdateReplicationPriorityAdminRequest.Action#UNSET}.</li>
   *   <li>{@code clear=true}, empty partitions → {@link UpdateReplicationPriorityAdminRequest.Action#UNSET_ALL}
   *       (the "servers required, no partitions" case — the servers requirement is enforced separately in
   *       {@link #resolveTargetHostPartitions}; the action itself is partition-agnostic).</li>
   * </ul>
   *
   * @param clear whether this is a clear (unset) operation.
   * @param partitionIds the partitions targeted (may be empty).
   * @param boost the boost weight (only meaningful on the SET path).
   * @return the resolved {@link UpdateReplicationPriorityAdminRequest.Action}.
   * @throws IllegalArgumentException if the partition-list shape or boost is invalid for the action.
   */
  @VisibleForTesting
  static UpdateReplicationPriorityAdminRequest.Action resolveAction(boolean clear, List<PartitionId> partitionIds,
      int boost) {
    boolean empty = partitionIds.isEmpty();
    if (!clear) {
      if (empty) {
        throw new IllegalArgumentException("SET (clear=false) requires a non-empty partition list");
      }
      if (boost < 1 || boost > UpdateReplicationPriorityAdminRequest.MAX_PRIORITY_BOOST) {
        throw new IllegalArgumentException(
            "boost must be in [1, " + UpdateReplicationPriorityAdminRequest.MAX_PRIORITY_BOOST + "] for SET; got "
                + boost);
      }
      if (partitionIds.size() > UpdateReplicationPriorityAdminRequest.MAX_PARTITIONS_PER_REQUEST) {
        throw new IllegalArgumentException(
            "partition list size " + partitionIds.size() + " exceeds cap "
                + UpdateReplicationPriorityAdminRequest.MAX_PARTITIONS_PER_REQUEST);
      }
      return UpdateReplicationPriorityAdminRequest.Action.SET;
    }
    // clear == true
    if (empty) {
      return UpdateReplicationPriorityAdminRequest.Action.UNSET_ALL;
    }
    if (partitionIds.size() > UpdateReplicationPriorityAdminRequest.MAX_PARTITIONS_PER_REQUEST) {
      throw new IllegalArgumentException(
          "partition list size " + partitionIds.size() + " exceeds cap "
              + UpdateReplicationPriorityAdminRequest.MAX_PARTITIONS_PER_REQUEST);
    }
    return UpdateReplicationPriorityAdminRequest.Action.UNSET;
  }

  /**
   * Resolves, for a fabric-scoped SET/UNSET/UNSET_ALL operation, the ordered map from each target host to the
   * subset of the requested partitions that host actually carries. Priority ops are ALWAYS fabric-scoped:
   * {@code fabrics} must be non-empty (an empty list is rejected). Pure function (reads only the
   * {@link ClusterMap}) so it is unit-testable against a controlled cluster layout.
   *
   * <p>Why a per-host subset rather than the full list to every host: for SET, the server handler is
   * all-or-nothing — it rejects the ENTIRE request with {@code BadRequest} if the receiving host does not
   * carry every listed partition (see {@code AmbryServerRequests.handleUpdateReplicationPriorityRequest}'s
   * {@code hostsPartition} check, which is gated on {@code action == SET}). Since Ambry spreads each partition
   * across only a subset of nodes per datacenter, a host that carries {@code p1} but not {@code p2} would
   * reject {@code {p1,p2}}. Sending each host only the partitions it hosts avoids those spurious rejections.
   * For UNSET the server does NOT validate hosting, so this is not a server requirement there; we apply the
   * same per-host subset deliberately as a harmless refinement (a host only clears priorities for partitions
   * it actually carries; clearing a non-present one would be a no-op).
   *
   * <p>The {@code (action, servers)} combinations map as follows:
   * <ul>
   *   <li>SET / UNSET, {@code servers} empty: each replica-hosting node (in {@code fabrics}) of any listed
   *       partition mapped to exactly the listed partitions it hosts (de-duplicated, first-seen order); hosts
   *       with no hosted partition are omitted.</li>
   *   <li>SET / UNSET, {@code servers} non-empty: same per-host subset, but restricted to the listed servers,
   *       after validating each server is in the fabric(s) AND hosts at least one requested partition
   *       (any failure rejects the whole request, no partial success).</li>
   *   <li>UNSET_ALL ({@code servers} REQUIRED, no partitions): each listed server (validated in-fabric) mapped
   *       to an empty list. An empty {@code servers} for UNSET_ALL is rejected.</li>
   * </ul>
   *
   * @param clusterMap the {@link ClusterMap} to resolve against.
   * @param fabrics the datacenter names to fan out to (REQUIRED — non-empty).
   * @param servers the operator-supplied host narrowing (may be empty for SET/UNSET; REQUIRED for UNSET_ALL).
   * @param partitionIds the partitions involved (empty for UNSET_ALL).
   * @param action the resolved {@link UpdateReplicationPriorityAdminRequest.Action} (SET / UNSET / UNSET_ALL).
   * @return an ordered map from target {@link DataNodeId} to the partitions to send that host.
   * @throws IllegalArgumentException if {@code fabrics} is empty, a fabric is unknown, a listed server fails
   *                                  validation, or UNSET_ALL is given with no servers.
   */
  @VisibleForTesting
  static Map<DataNodeId, List<PartitionId>> resolveTargetHostPartitions(ClusterMap clusterMap, List<String> fabrics,
      List<String> servers, List<PartitionId> partitionIds, UpdateReplicationPriorityAdminRequest.Action action) {
    if (fabrics.isEmpty()) {
      throw new IllegalArgumentException(
          "--fabric is REQUIRED for SetReplicationPriority (no implicit cluster-wide default); specify the "
              + "target fabric explicitly");
    }
    Set<String> fabricSet = validateAndCollectFabrics(clusterMap, fabrics);
    boolean serversProvided = !servers.isEmpty();
    Map<DataNodeId, List<PartitionId>> hostPartitions = new LinkedHashMap<>();

    if (action == UpdateReplicationPriorityAdminRequest.Action.UNSET_ALL) {
      // clear=true, partitions empty -> servers REQUIRED. No partition-hosting check (no partitions).
      if (!serversProvided) {
        throw new IllegalArgumentException(
            "Clear with no partitions (UNSET_ALL) requires --servers; refusing to clear all priorities "
                + "across whole fabric(s)");
      }
      for (String server : servers) {
        DataNodeId node = resolveServerInFabrics(clusterMap, server, fabricSet);
        hostPartitions.put(node, Collections.emptyList());
      }
      return hostPartitions;
    }

    // SET / UNSET. Build the per-host hosted-subset map over the fabric(s).
    Map<DataNodeId, List<PartitionId>> fabricHostSubset = new LinkedHashMap<>();
    for (PartitionId partitionId : partitionIds) {
      for (ReplicaId replicaId : partitionId.getReplicaIds()) {
        DataNodeId node = replicaId.getDataNodeId();
        if (fabricSet.contains(node.getDatacenterName())) {
          List<PartitionId> forHost = fabricHostSubset.computeIfAbsent(node, n -> new ArrayList<>());
          if (!forHost.contains(partitionId)) {
            forHost.add(partitionId);
          }
        }
      }
    }

    if (!serversProvided) {
      // servers omitted -> every fabric host hosting a requested partition; reject if none do (e.g. a mistyped
      // --fabric) rather than silently doing nothing.
      if (fabricHostSubset.isEmpty()) {
        throw new IllegalArgumentException(
            "None of the requested partitions are hosted in fabric(s) " + fabricSet
                + "; nothing to do (check --fabric and --partition.ids)");
      }
      return fabricHostSubset;
    }

    // servers provided -> validate each is in-fabric AND hosts at least one requested partition;
    // ANY failure rejects the WHOLE request (no partial).
    for (String server : servers) {
      DataNodeId node = resolveServerInFabrics(clusterMap, server, fabricSet);
      List<PartitionId> hosted = fabricHostSubset.get(node);
      if (hosted == null || hosted.isEmpty()) {
        throw new IllegalArgumentException(
            "Server " + server + " hosts none of the requested partitions in the given fabric(s); whole request "
                + "rejected (no partial success)");
      }
      hostPartitions.put(node, hosted);
    }
    return hostPartitions;
  }

  /**
   * Resolves a single operator-supplied server hostname to its {@link DataNodeId}, validating that it exists
   * and lives in one of the requested fabrics. Used by both the set/clear and list paths so the
   * "server must be in fabric" rule is enforced uniformly.
   * @param clusterMap the {@link ClusterMap} to resolve against.
   * @param hostname the operator-supplied hostname.
   * @param fabricSet the requested fabric(s).
   * @return the resolved {@link DataNodeId}.
   * @throws IllegalArgumentException if the host is unknown or not in any requested fabric.
   */
  private static DataNodeId resolveServerInFabrics(ClusterMap clusterMap, String hostname, Set<String> fabricSet) {
    DataNodeId resolved = null;
    for (DataNodeId node : clusterMap.getDataNodeIds()) {
      if (node.getHostname().equals(hostname)) {
        resolved = node;
        break;
      }
    }
    if (resolved == null) {
      throw new IllegalArgumentException("Unknown server (no data node with hostname): " + hostname);
    }
    if (!fabricSet.contains(resolved.getDatacenterName())) {
      throw new IllegalArgumentException(
          "Server " + hostname + " is in fabric " + resolved.getDatacenterName() + ", not in the requested fabric(s) "
              + fabricSet);
    }
    return resolved;
  }

  /**
   * Resolves the set of target hosts for the partition-agnostic List operation. Unlike set/clear, List
   * does NOT require {@code fabrics}: when both {@code fabrics} and {@code servers} are empty it fans out to
   * EVERY host in the cluster. When {@code servers} is supplied it restricts to those hosts (validated against
   * the requested fabric(s) if any were given, else any fabric). Pure function (reads only the
   * {@link ClusterMap}) so it is unit-testable against a {@code MockClusterMap}.
   *
   * @param clusterMap the {@link ClusterMap} to resolve against.
   * @param fabrics the datacenter names to scope to (optional; empty = whole cluster).
   * @param servers the host narrowing (optional; empty = all hosts in scope).
   * @return the de-duplicated list of target {@link DataNodeId}s, first-seen order.
   * @throws IllegalArgumentException if a fabric is unknown, or a listed server is unknown / out of fabric scope.
   */
  @VisibleForTesting
  static List<DataNodeId> resolveTargetHosts(ClusterMap clusterMap, List<String> fabrics, List<String> servers) {
    Set<String> fabricSet = fabrics.isEmpty() ? allFabrics(clusterMap) : validateAndCollectFabrics(clusterMap, fabrics);
    LinkedHashSet<DataNodeId> hosts = new LinkedHashSet<>();
    if (!servers.isEmpty()) {
      for (String server : servers) {
        hosts.add(resolveServerInFabrics(clusterMap, server, fabricSet));
      }
      return new ArrayList<>(hosts);
    }
    for (DataNodeId node : clusterMap.getDataNodeIds()) {
      if (fabricSet.contains(node.getDatacenterName())) {
        hosts.add(node);
      }
    }
    return new ArrayList<>(hosts);
  }

  /** Returns every datacenter (fabric) name present in the cluster map. */
  private static Set<String> allFabrics(ClusterMap clusterMap) {
    Set<String> dcs = new LinkedHashSet<>();
    for (DataNodeId node : clusterMap.getDataNodeIds()) {
      dcs.add(node.getDatacenterName());
    }
    return dcs;
  }

  /**
   * Validates that every requested fabric exists in the cluster map and returns the fabric set (preserving
   * the requested order).
   * @param clusterMap the {@link ClusterMap} to resolve against.
   * @param fabrics the requested datacenter names.
   * @return the de-duplicated set of fabrics.
   * @throws IllegalArgumentException if any requested fabric is not a known datacenter.
   */
  private static Set<String> validateAndCollectFabrics(ClusterMap clusterMap, List<String> fabrics) {
    Set<String> knownDcs = new LinkedHashSet<>();
    for (DataNodeId node : clusterMap.getDataNodeIds()) {
      knownDcs.add(node.getDatacenterName());
    }
    for (String fabric : fabrics) {
      if (!knownDcs.contains(fabric)) {
        throw new IllegalArgumentException("Unknown fabric (datacenter): " + fabric + "; known: " + knownDcs);
      }
    }
    return new LinkedHashSet<>(fabrics);
  }

  /**
   * Parses the raw {@code partition.ids} config string into resolved {@link PartitionId}s. Comma-splitting,
   * blank-token skipping, and per-token trimming are delegated to {@link Utils#splitString(String, String,
   * java.util.function.Function, java.util.Collection)} (so stray/trailing commas and surrounding spaces do not
   * produce a spurious "Partition Id is not valid" error). An unknown/unresolvable id is a hard error.
   * @param partitionIdsRaw the raw comma-separated config value (the default-empty config {@code ""} yields an
   *                        empty list).
   * @param clusterMap the {@link ClusterMap} to resolve against.
   * @return the resolved {@link PartitionId}s (possibly empty).
   * @throws IllegalArgumentException if any non-blank token does not resolve to a known partition.
   */
  @VisibleForTesting
  static List<PartitionId> parsePartitionIds(String partitionIdsRaw, ClusterMap clusterMap) {
    List<PartitionId> partitionIds = new ArrayList<>();
    for (String token : splitTrimmed(partitionIdsRaw)) {
      partitionIds.add(ServerAdminTool.getPartitionIdFromStr(token, clusterMap));
    }
    return partitionIds;
  }

  /**
   * Splits a raw comma-separated config value into trimmed, non-blank tokens via
   * {@link Utils#splitString(String, String, java.util.function.Function, java.util.Collection)} (the filter
   * trims each token and drops it if blank). The default-empty config {@code ""} yields an empty list.
   * @param raw the raw comma-separated config value.
   * @return the cleaned token list (possibly empty).
   */
  private static List<String> splitTrimmed(String raw) {
    List<String> tokens = new ArrayList<>();
    Utils.splitString(raw, ",", token -> {
      String trimmed = token.trim();
      return trimmed.isEmpty() ? null : trimmed;
    }, tokens);
    return tokens;
  }

  /**
   * Drives a {@code SetReplicationPriority} operation: resolves the action and fabric-scoped target
   * hosts (optionally narrowed by {@code servers}), enforces the {@code maxFanoutTotal} ceiling, fans the
   * request out to each host (per-host failure does not abort the others), and prints the JSON result
   * body to stdout. Admissibility failures (missing fabric, bad servers, boost out of range, fan-out cap
   * exceeded, clear-with-neither) propagate as {@link IllegalArgumentException} so the caller exits non-zero
   * WITHOUT printing a results body.
   * @param partitionIdsRaw the raw {@code partition.ids} config value.
   * @param boost the boost weight (only meaningful on the SET path).
   * @param clear whether this is a clear (unset) operation.
   * @param fabricConfig the raw single {@code fabric} config value.
   * @param serversRaw the raw {@code servers} config value.
   * @param maxFanoutTotal the configured {@code partitions × hosts} ceiling.
   */
  public void runSetReplicationPriority(String partitionIdsRaw, int boost, boolean clear, String fabricConfig,
      String serversRaw, int maxFanoutTotal) {
    List<PartitionId> partitionIds = parsePartitionIds(partitionIdsRaw, clusterMap);
    UpdateReplicationPriorityAdminRequest.Action action = resolveAction(clear, partitionIds, boost);
    List<String> fabrics = toFabricList(fabricConfig);
    List<String> servers = splitTrimmed(serversRaw);
    // Each host gets only the requested partitions it actually carries. For SET this avoids the server's
    // all-or-nothing rejection; for UNSET the server doesn't validate hosting, so it's a harmless refinement.
    Map<DataNodeId, List<PartitionId>> hostPartitions =
        resolveTargetHostPartitions(clusterMap, fabrics, servers, partitionIds, action);
    // maxFanoutTotal admission guard: partitions × resolvedHosts. UNSET_ALL carries no partitions, so its
    // fan-out cost is one unit per host (each host clears its whole map in one request).
    int partitionFactor = partitionIds.isEmpty() ? 1 : partitionIds.size();
    checkMaxFanoutTotal(partitionFactor, hostPartitions.size(), maxFanoutTotal);
    // boost is only meaningful for SET; for UNSET/UNSET_ALL the server ignores it, so don't log a misleading value.
    String boostToken = action == UpdateReplicationPriorityAdminRequest.Action.SET ? "boost=" + boost : "boost=n/a";
    LOGGER.info("SetReplicationPriority action={} {} partitions={} targeting {} host(s) (maxFanoutTotal={})", action,
        boostToken, partitionIds, hostPartitions.size(), maxFanoutTotal);
    Map<DataNodeId, String> failures = new LinkedHashMap<>();
    FanOutSummary summary = fanOut(new ArrayList<>(hostPartitions.keySet()), action.toString(),
        host -> sendSetForHost(host, hostPartitions.get(host), boost, action), failures);
    System.out.println(buildSetResultJson(hostPartitions.keySet(), failures, summary).toString(2));
  }

  /**
   * Sends a single host its {@code SET}/{@code UNSET}/{@code UNSET_ALL} request and converts a non-success
   * {@link ServerErrorCode} into an {@link IOException} so {@link #fanOut} can treat error codes and transport
   * failures uniformly (both count as that host's failure).
   * @param host the target host.
   * @param partitionsForHost the partitions to send this host (the subset it actually carries).
   * @param boost the boost weight (only meaningful for {@code SET}).
   * @param action the {@link UpdateReplicationPriorityAdminRequest.Action} to perform.
   * @throws IOException on a transport/parse failure, or when the host returns a non-{@code NoError} code.
   * @throws TimeoutException if the host did not respond in time.
   */
  private void sendSetForHost(DataNodeId host, List<PartitionId> partitionsForHost, int boost,
      UpdateReplicationPriorityAdminRequest.Action action) throws IOException, TimeoutException {
    ServerErrorCode errorCode = updateReplicationPriority(host, partitionsForHost, boost, action);
    if (errorCode != ServerErrorCode.NoError) {
      throw new IOException("ServerErrorCode: " + errorCode);
    }
  }

  /**
   * Builds the {@link UpdateReplicationPriorityAdminRequest} sent to a single host. The partition list, boost,
   * and action are carried in the typed request body; the wrapping {@link AdminRequest} is built with a
   * {@code null} partition (the server reads partitions from the typed body, not the header). Pure function so
   * the wire-request shape is unit-testable without a network.
   * @param partitionIds the partitions to operate on (empty for {@code UNSET_ALL}).
   * @param boost the boost weight (only meaningful for {@code SET}).
   * @param action the {@link UpdateReplicationPriorityAdminRequest.Action} to perform.
   * @param correlationId the correlation id for the wrapping {@link AdminRequest}.
   * @return the constructed {@link UpdateReplicationPriorityAdminRequest}.
   */
  @VisibleForTesting
  static UpdateReplicationPriorityAdminRequest buildUpdateRequest(List<PartitionId> partitionIds, int boost,
      UpdateReplicationPriorityAdminRequest.Action action, int correlationId) {
    AdminRequest adminRequest =
        new AdminRequest(AdminRequestOrResponseType.UpdateReplicationPriority, null, correlationId, CLIENT_ID);
    return new UpdateReplicationPriorityAdminRequest(partitionIds, boost, action, adminRequest);
  }

  /**
   * Chooses which partition (if any) to use for replica resolution when sending to a single host. For a
   * single-partition list the replica is resolved precisely; otherwise (UNSET_ALL / multi-partition) any
   * replica on the node is fine since the request is not bound to one partition, so {@code null} is returned
   * and {@code getReplicaFromNode} picks any replica on the node. Pure function so it is unit-testable.
   * @param partitionIds the partitions in the request (may be empty).
   * @return the single partition to resolve against, or {@code null} for the empty/multi-partition cases.
   */
  @VisibleForTesting
  static PartitionId resolveReplicaPartition(List<PartitionId> partitionIds) {
    return partitionIds.size() == 1 ? partitionIds.get(0) : null;
  }

  /**
   * Runs {@code work} against each host, isolating per-host failures: one host's thrown {@link IOException} or
   * {@link TimeoutException} does not abort the others. Records each failing host's error message in
   * {@code failures} (insertion-ordered) so the caller can emit the per-host {@code status:"error"} JSON, and
   * returns the {@code requested/succeeded/failed} counts. Used by both the set/clear path (whose {@code work}
   * lambda turns a non-{@code NoError} {@link ServerErrorCode} into an {@link IOException}) and the list path.
   * @param hosts the target hosts, in fan-out order.
   * @param operationName the operation name (for logging only).
   * @param work the per-host unit of work (a thrown exception counts as a failure for that host only).
   * @param failures populated with each failing host's error message.
   * @return the {@link FanOutSummary} of the fan-out.
   */
  @VisibleForTesting
  static FanOutSummary fanOut(List<DataNodeId> hosts, String operationName, HostWork work,
      Map<DataNodeId, String> failures) {
    int succeeded = 0;
    for (DataNodeId host : hosts) {
      try {
        work.run(host);
        succeeded++;
        LOGGER.info("{}: {} succeeded", host, operationName);
      } catch (Exception e) {
        // Deliberately broad: this is the per-host failure-isolation boundary, so it must catch EVERY way a
        // single host can fail without aborting the others. That includes the checked IOException/TimeoutException
        // the work lambdas declare AND unchecked failures from the transport — notably the unreachable/errored-host
        // case, where ServerAdminTool.sendRequestGetResponse throws an unchecked IllegalStateException. The
        // narrow-exception guideline does not apply here precisely because the contract is "isolate any per-host
        // failure".
        failures.put(host, e.getClass().getSimpleName() + ": " + e.getMessage());
        LOGGER.error("{}: {} failed", host, operationName, e);
      }
    }
    return new FanOutSummary(hosts.size(), succeeded);
  }

  /** Immutable summary of a per-host fan-out: how many hosts were targeted, succeeded, and failed. */
  @VisibleForTesting
  static final class FanOutSummary {
    final int requested;
    final int succeeded;
    final int failed;

    FanOutSummary(int requested, int succeeded) {
      this.requested = requested;
      this.succeeded = succeeded;
      this.failed = requested - succeeded;
    }
  }

  /**
   * Drives a {@code ListReplicationPriority} operation: resolves target hosts (every host in the
   * cluster, or the {@code fabric}/{@code servers}-scoped subset — {@code fabric} is NOT required for
   * list), enforces the {@code maxFanoutTotal} ceiling against the resolved host count (list is
   * partition-agnostic, so the partition factor is 1), queries each (per-host failure does not abort the
   * others), applies the optional {@code partition.ids} filter, and prints the JSON response body to stdout.
   * @param partitionIdsRaw the raw {@code partition.ids} config value (optional post-fan-out filter).
   * @param fabricConfig the raw single {@code fabric} config value (optional scope).
   * @param serversRaw the raw {@code servers} config value (optional scope).
   * @param maxFanoutTotal the configured host-count ceiling (an unscoped whole-cluster list above this is
   *                       rejected before any host is contacted).
   */
  public void runListReplicationPriority(String partitionIdsRaw, String fabricConfig, String serversRaw,
      int maxFanoutTotal) {
    List<String> fabrics = toFabricList(fabricConfig);
    List<String> servers = splitTrimmed(serversRaw);
    List<DataNodeId> hosts = resolveTargetHosts(clusterMap, fabrics, servers);
    // List is partition-agnostic, so its fan-out cost is one unit per host. Guard against an unscoped
    // whole-cluster list contacting every node above the cap.
    checkMaxFanoutTotal(1, hosts.size(), maxFanoutTotal);
    // Optional partition filter applied at the tool after fan-out. Empty = no filter.
    Set<PartitionId> partitionFilter = new LinkedHashSet<>(parsePartitionIds(partitionIdsRaw, clusterMap));
    LOGGER.info("ListReplicationPriority targeting {} host(s){}", hosts.size(),
        partitionFilter.isEmpty() ? "" : " (filtering to partitions=" + partitionFilter + ")");

    // Per-host outcome accumulators, kept in fan-out order for a stable JSON results[] array.
    Map<DataNodeId, List<PriorityEntry>> hostEntries = new LinkedHashMap<>();
    Map<DataNodeId, String> failures = new LinkedHashMap<>();
    FanOutSummary summary = fanOut(hosts, "ListReplicationPriority", host -> {
      // A replica-less node (spare/draining) hosts no partitions, so it has no priorities. Report it as empty/OK
      // rather than contacting it (which would surface a healthy node as an error).
      List<PriorityEntry> entries =
          clusterMap.getReplicaIds(host).isEmpty() ? Collections.emptyList() : listReplicationPriority(host);
      hostEntries.put(host, entries);
    }, failures);
    System.out.println(buildListResultJson(hosts, hostEntries, failures, partitionFilter, summary).toString(2));
  }

  /**
   * Builds the set/clear JSON response body: {@code {summary:{requested,succeeded,failed}, results:[...]}}.
   * Each host appears once in {@code results}; a host in {@code failures} gets {@code status:"error"} with its
   * {@code error} message, otherwise {@code status:"ok"}. Pure function so the shape is unit-testable.
   * @param hosts the hosts that were targeted (insertion order preserved for stable output).
   * @param failures per-host error messages for the hosts that failed.
   * @param summary the {@link FanOutSummary} counts.
   * @return the assembled {@link JSONObject}.
   */
  @VisibleForTesting
  static JSONObject buildSetResultJson(Set<DataNodeId> hosts, Map<DataNodeId, String> failures,
      FanOutSummary summary) {
    JSONObject root = new JSONObject();
    JSONObject summaryJson = new JSONObject();
    summaryJson.put("requested", summary.requested);
    summaryJson.put("succeeded", summary.succeeded);
    summaryJson.put("failed", summary.failed);
    root.put("summary", summaryJson);
    JSONArray results = new JSONArray();
    for (DataNodeId host : hosts) {
      JSONObject hostJson = new JSONObject();
      hostJson.put("host", host.getHostname());
      String error = failures.get(host);
      if (error == null) {
        hostJson.put("status", "ok");
      } else {
        hostJson.put("status", "error");
        hostJson.put("error", error);
      }
      results.put(hostJson);
    }
    root.put("results", results);
    return root;
  }

  /**
   * Builds the list JSON response body:
   * {@code {summary:{queriedHosts,hostsWithPriorities,uniquePartitions}, partitions:[...], servers:[...],
   * results:[{host,status,priorities:[{partitionId,boost,interColo}]}|{host,status:error,error}]}}.
   * The server returns one {@link PriorityEntry} per ReplicaThread holding the partition, so a partition can
   * legitimately appear multiple times on one host (e.g. one intra-colo entry with {@code interColo=false} and
   * one or more inter-colo entries with {@code interColo=true}); each entry is emitted as its own row in the
   * order the server returned them, never collapsed. {@code partitions} is the dedup of DISTINCT partition ids
   * across hosts (a partition counts once even if it has both an intra and an inter entry); {@code servers} is
   * the hosts whose (post-filter) priority list is non-empty; error hosts are excluded from both derived lists
   * but appear in {@code results} with {@code status:"error"}. Pure function so the shape is unit-testable.
   * @param hosts the queried hosts, in fan-out order.
   * @param hostEntries the raw per-host entries returned (absent for hosts that errored).
   * @param failures per-host error messages for hosts that failed.
   * @param partitionFilter the optional partition filter (empty = keep all).
   * @param summary the {@link FanOutSummary} counts.
   * @return the assembled {@link JSONObject}.
   */
  @VisibleForTesting
  static JSONObject buildListResultJson(List<DataNodeId> hosts, Map<DataNodeId, List<PriorityEntry>> hostEntries,
      Map<DataNodeId, String> failures, Set<PartitionId> partitionFilter, FanOutSummary summary) {
    JSONArray results = new JSONArray();
    Set<Long> uniquePartitions = new LinkedHashSet<>();
    List<String> serversWithPriorities = new ArrayList<>();
    int hostsWithPriorities = 0;
    for (DataNodeId host : hosts) {
      JSONObject hostJson = new JSONObject();
      hostJson.put("host", host.getHostname());
      String error = failures.get(host);
      if (error != null) {
        hostJson.put("status", "error");
        hostJson.put("error", error);
        results.put(hostJson);
        continue;
      }
      hostJson.put("status", "ok");
      JSONArray priorities = new JSONArray();
      for (PriorityEntry entry : hostEntries.getOrDefault(host, Collections.emptyList())) {
        PartitionId partitionId = entry.getPartitionId();
        if (!partitionFilter.isEmpty() && !partitionFilter.contains(partitionId)) {
          continue;
        }
        JSONObject rowJson = new JSONObject();
        rowJson.put("partitionId", partitionId.getId());
        rowJson.put("boost", entry.getBoost());
        rowJson.put("interColo", entry.isInterColo());
        priorities.put(rowJson);
        uniquePartitions.add(partitionId.getId());
      }
      hostJson.put("priorities", priorities);
      results.put(hostJson);
      if (priorities.length() > 0) {
        hostsWithPriorities++;
        serversWithPriorities.add(host.getHostname());
      }
    }
    JSONObject root = new JSONObject();
    JSONObject summaryJson = new JSONObject();
    summaryJson.put("queriedHosts", summary.requested);
    summaryJson.put("hostsWithPriorities", hostsWithPriorities);
    summaryJson.put("uniquePartitions", uniquePartitions.size());
    root.put("summary", summaryJson);
    JSONArray partitionsJson = new JSONArray();
    for (Long partition : uniquePartitions) {
      partitionsJson.put(partition.longValue());
    }
    root.put("partitions", partitionsJson);
    JSONArray serversJson = new JSONArray();
    for (String server : serversWithPriorities) {
      serversJson.put(server);
    }
    root.put("servers", serversJson);
    root.put("results", results);
    return root;
  }

  /**
   * Enforces the {@code maxFanoutTotal} ceiling: rejects if {@code partitionFactor × resolvedHosts}
   * exceeds the cap, BEFORE any host is contacted.
   * @param partitionFactor the per-host partition multiplier (partition count, or 1 for UNSET_ALL / list).
   * @param resolvedHosts the number of hosts the request resolved to.
   * @param maxFanoutTotal the configured ceiling.
   * @throws IllegalArgumentException if the product exceeds {@code maxFanoutTotal}.
   */
  @VisibleForTesting
  static void checkMaxFanoutTotal(int partitionFactor, int resolvedHosts, int maxFanoutTotal) {
    long total = (long) partitionFactor * resolvedHosts;
    if (total > maxFanoutTotal) {
      throw new IllegalArgumentException(
          "partitions × resolvedHosts = " + partitionFactor + " × " + resolvedHosts + " = " + total
              + " exceeds maxFanoutTotal=" + maxFanoutTotal + "; raise --max.fanout.total to override");
    }
  }

  /**
   * Converts the raw single {@code fabric} config value into a clean list: trimmed and (if non-blank)
   * wrapped in a singleton list so the downstream list-based resolvers still apply the "fabric required",
   * "fabric must be a known DC", and "server must be in fabric" rules. Only one fabric may be specified per
   * invocation (scope/blast-radius control), so a comma-separated value is rejected. A blank value yields an
   * empty list (no fabric scope). Genuinely unknown fabrics are still rejected downstream in
   * {@link #resolveTargetHostPartitions} / {@link #resolveTargetHosts}.
   * @param fabric the raw config value (a single fabric name, or empty).
   * @return the cleaned single-element fabric list, or empty if no fabric was given.
   * @throws IllegalArgumentException if {@code fabric} contains a comma (more than one fabric requested).
   */
  @VisibleForTesting
  static List<String> toFabricList(String fabric) {
    if (fabric.contains(",")) {
      throw new IllegalArgumentException(
          "only one fabric may be specified per invocation; run one fabric at a time (got: " + fabric + ")");
    }
    String trimmed = fabric.trim();
    return trimmed.isEmpty() ? Collections.emptyList() : Collections.singletonList(trimmed);
  }

  /**
   * Sends an {@link UpdateReplicationPriorityAdminRequest} to {@code dataNodeId}. The partition list, boost,
   * and action are carried in the typed request body; the wrapping {@link AdminRequest} is built with a
   * {@code null} partition (the server reads partitions from the typed body, not the header).
   * @param dataNodeId the {@link DataNodeId} to contact.
   * @param partitionIds the partitions to operate on (empty for {@code UNSET_ALL}).
   * @param boost the boost weight (only meaningful for {@code SET}).
   * @param action the {@link UpdateReplicationPriorityAdminRequest.Action} to perform.
   * @return the {@link ServerErrorCode} that is returned.
   * @throws IOException if the response could not be read or parsed.
   * @throws TimeoutException if the host did not respond in time.
   */
  private ServerErrorCode updateReplicationPriority(DataNodeId dataNodeId, List<PartitionId> partitionIds, int boost,
      UpdateReplicationPriorityAdminRequest.Action action) throws IOException, TimeoutException {
    UpdateReplicationPriorityAdminRequest updateRequest =
        buildUpdateRequest(partitionIds, boost, action, correlationId.incrementAndGet());
    PartitionId replicaResolutionPartition = resolveReplicaPartition(partitionIds);
    ResponseInfo response = sender.send(dataNodeId, replicaResolutionPartition, updateRequest);
    if (response == null) {
      throw new IOException("Null response from " + dataNodeId + " — possible network-layer failure");
    }
    try {
      AdminResponse adminResponse = AdminResponse.readFrom(new NettyByteBufDataInputStream(response.content()));
      return adminResponse.getError();
    } finally {
      response.release();
    }
  }

  /**
   * Sends a {@link ListReplicationPriorityAdminRequest} to {@code dataNodeId} and returns the priority
   * snapshot held by the host's replication engine.
   * @param dataNodeId the {@link DataNodeId} to contact.
   * @return the list of {@link PriorityEntry}s the host reports.
   * @throws IOException if the response could not be read or parsed, or the host returned a server error.
   * @throws TimeoutException if the host did not respond in time.
   */
  private List<PriorityEntry> listReplicationPriority(DataNodeId dataNodeId) throws IOException, TimeoutException {
    AdminRequest adminRequest =
        new AdminRequest(AdminRequestOrResponseType.ListReplicationPriority, null, correlationId.incrementAndGet(),
            CLIENT_ID);
    ListReplicationPriorityAdminRequest listRequest = new ListReplicationPriorityAdminRequest(adminRequest);
    // Partition-agnostic request: pass null so getReplicaFromNode picks any replica on the node.
    ResponseInfo response = sender.send(dataNodeId, null, listRequest);
    if (response == null) {
      throw new IOException("Null response from " + dataNodeId + " — possible network-layer failure");
    }
    try {
      ListReplicationPriorityAdminResponse adminResponse =
          ListReplicationPriorityAdminResponse.readFrom(new NettyByteBufDataInputStream(response.content()),
              clusterMap);
      ServerErrorCode errorCode = adminResponse.getError();
      if (errorCode != ServerErrorCode.NoError) {
        throw new IOException("ListReplicationPriority from " + dataNodeId + " returned server error " + errorCode);
      }
      return adminResponse.getEntries();
    } finally {
      response.release();
    }
  }
}
