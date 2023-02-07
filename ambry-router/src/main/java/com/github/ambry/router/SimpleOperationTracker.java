/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.router;

import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaState;
import com.github.ambry.commons.BlobId;
import com.github.ambry.config.RouterConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A implementation of {@link OperationTracker}. It internally maintains the status of a
 * corresponding operation, and returns information that decides if the operation should
 * continue or terminate.
 *
 * This implementation simplifies such that it unifies parallelism. That is, a single parallelism
 * parameter controls the maximum number of total allowed in-flight requests to both local and remote
 * replicas. This simplification is valid for PUT operation, yet a mature implementation will take
 * a more sophisticated control of parallelism in the future.
 *
 * This class assumes a request will be {@code succeeded, failed, or timedout} (which means failed).
 * So a deterministic response will be received in a definite time, and no request will pend forever.
 * When a request is timed out, it is considered as failed.
 *
 * A typical usage of an {@code SimpleOperationTracker} would be:
 *<pre>
 *{@code
 *
 *   SimpleOperationTracker operationTracker = new SimpleOperationTracker(datacenterName,
 *            partitionId, crossColoEnabled, successTarget, parallelism);
 *   //...
 *   Iterator<ReplicaId> itr = operationTracker.getReplicaIterator();
 *   while (itr.hasNext()) {
 *     ReplicaId nextReplica = itr.next();
 *     //determine request can be sent to the replica, i.e., connection available.
 *     if(true) {
 *       itr.remove();
 *     }
 *   }
 *}
 *</pre>
 *
 */
class SimpleOperationTracker implements OperationTracker {
  private static final Logger logger = LoggerFactory.getLogger(SimpleOperationTracker.class);
  private static final int NOT_FOUND_THRESHOLD_INVALID_VALUE = -1;
  protected final String datacenterName;
  protected final String originatingDcName;
  protected final int replicaSuccessTarget;
  protected final int replicaParallelism;
  // How many NotFound responses from originating dc will terminate the operation.
  protected int originatingDcNotFoundFailureThreshold = NOT_FOUND_THRESHOLD_INVALID_VALUE;
  protected final int totalReplicaCount;
  protected final LinkedList<ReplicaId> replicaPool = new LinkedList<>();
  protected final NonBlockingRouterMetrics routerMetrics;
  private final OpTrackerIterator otIterator;
  private final RouterOperation routerOperation;
  private final PartitionId partitionId;
  private final RouterConfig routerConfig;
  protected int inflightCount = 0;
  protected int replicaSuccessCount = 0;
  protected List<ReplicaId> successReplica = new ArrayList<>();
  protected int replicaInPoolOrFlightCount = 0;
  protected int failedCount = 0;
  protected int disabledCount = 0;
  protected int originatingDcNotFoundCount = 0;
  protected int totalNotFoundCount = 0;
  protected ReplicaId lastReturnedByIterator = null;
  private Iterator<ReplicaId> replicaIterator;
  private final Set<ReplicaId> originatingDcLeaderOrStandbyReplicas;
  private final int originatingDcTotalReplicaCount;
  private final Map<ReplicaState, List<ReplicaId>> allDcReplicasByState;
  private final BlobId blobId;

  /**
   * Constructor for an {@code SimpleOperationTracker}. In constructor, there is a config allowing operation tracker to
   * use eligible replicas to populate replica pool. ("eligible" replicas are those in required states for specific
   * operation)
   * Following are different types of operation and their eligible replica states:
   *  ---------------------------------------------------------
   * |  Operation Type  |        Eligible Replica State        |
   *  ---------------------------------------------------------
   * |      PUT         | STANDBY, LEADER                      |
   * |      GET         | STANDBY, LEADER, BOOTSTRAP, INACTIVE |
   * |    DELETE        | STANDBY, LEADER, BOOTSTRAP           |
   * |   TTLUpdate      | STANDBY, LEADER, BOOTSTRAP           |
   * |   UNDELETE       | STANDBY, LEADER, BOOTSTRAP           |
   *  ---------------------------------------------------------
   * Following are dynamic configs when replica state is taken into consideration: (N is number of eligible replicas)
   *  -----------------------------------------------------------------------
   * |  Operation Type  |        Parallelism              |  Success Target  |
   *  -----------------------------------------------------------------------
   * |     GET          | 1~2 decided by adaptive tracker |         1        |
   * |     PUT          |           N                     |       N - 1      |
   * |    DELETE        |          3~N                    |         2        |
   * |   TTLUpdate      |          3~N                    |         2        |
   * |   UNDELETE       |          3~N                    |  Global Quorum   |
   *  -----------------------------------------------------------------------
   *  Note: for now, we still use 3 as parallelism for DELETE/TTLUpdate/UNDELETE even though there are N eligible replicas, this
   *        can be adjusted to any number between 3 and N (inclusive)
   *        For Undelete, it needs to reach global quorum to succeed. A dedicated operation tracker is created to check that.
   * @param routerConfig The {@link RouterConfig} containing the configs for operation tracker.
   * @param routerOperation The {@link RouterOperation} which {@link SimpleOperationTracker} is associated with.
   * @param partitionId The partition on which the operation is performed.
   * @param originatingDcName The original DC where blob was put.
   * @param shuffleReplicas Indicates if the replicas need to be shuffled.
   * @param routerMetrics The {@link NonBlockingRouterMetrics} to use.
   */
  SimpleOperationTracker(RouterConfig routerConfig, RouterOperation routerOperation, PartitionId partitionId,
      String originatingDcName, boolean shuffleReplicas, NonBlockingRouterMetrics routerMetrics) {
    this(routerConfig, routerOperation, partitionId, originatingDcName, shuffleReplicas, routerMetrics, null);
  }

  /**
   * Constructor for an {@code SimpleOperationTracker}. In constructor, there is a config allowing operation tracker to
   * use eligible replicas to populate replica pool. ("eligible" replicas are those in required states for specific
   * operation)
   * Following are different types of operation and their eligible replica states:
   *  ---------------------------------------------------------
   * |  Operation Type  |        Eligible Replica State        |
   *  ---------------------------------------------------------
   * |      PUT         | STANDBY, LEADER                      |
   * |      GET         | STANDBY, LEADER, BOOTSTRAP, INACTIVE |
   * |    DELETE        | STANDBY, LEADER, BOOTSTRAP           |
   * |   TTLUpdate      | STANDBY, LEADER, BOOTSTRAP           |
   * |   UNDELETE       | STANDBY, LEADER, BOOTSTRAP           |
   *  ---------------------------------------------------------
   * Following are dynamic configs when replica state is taken into consideration: (N is number of eligible replicas)
   *  -----------------------------------------------------------------------
   * |  Operation Type  |        Parallelism              |  Success Target  |
   *  -----------------------------------------------------------------------
   * |     GET          | 1~2 decided by adaptive tracker |         1        |
   * |     PUT          |           N                     |       N - 1      |
   * |    DELETE        |          3~N                    |         2        |
   * |   TTLUpdate      |          3~N                    |         2        |
   * |   UNDELETE       |          3~N                    |  Global Quorum   |
   *  -----------------------------------------------------------------------
   *  Note: for now, we still use 3 as parallelism for DELETE/TTLUpdate/UNDELETE even though there are N eligible replicas, this
   *        can be adjusted to any number between 3 and N (inclusive)
   *        For Undelete, it needs to reach global quorum to succeed. A dedicated operation tracker is created to check that.
   * @param routerConfig The {@link RouterConfig} containing the configs for operation tracker.
   * @param routerOperation The {@link RouterOperation} which {@link SimpleOperationTracker} is associated with.
   * @param partitionId The partition on which the operation is performed.
   * @param originatingDcName The original DC where blob was put.
   * @param shuffleReplicas Indicates if the replicas need to be shuffled.
   * @param routerMetrics The {@link NonBlockingRouterMetrics} to use.
   * @param blobId The {@link BlobId}, if available, for the operation.
   */
  SimpleOperationTracker(RouterConfig routerConfig, RouterOperation routerOperation, PartitionId partitionId,
      String originatingDcName, boolean shuffleReplicas, NonBlockingRouterMetrics routerMetrics, BlobId blobId) {
    // populate tracker parameters based on operation type
    this.routerConfig = routerConfig;
    this.routerOperation = routerOperation;
    this.originatingDcName = originatingDcName;
    this.partitionId = partitionId;
    this.routerMetrics = routerMetrics;
    this.blobId = blobId;
    datacenterName = routerConfig.routerDatacenterName;

    // Note that we get a snapshot of replicas by state only once in this class, and use the same snapshot everywhere
    // to avoid the case where a replica state might change in between an operation.
    allDcReplicasByState =
        (Map<ReplicaState, List<ReplicaId>>) partitionId.getReplicaIdsByStates(EnumSet.allOf(ReplicaState.class), null);
    List<ReplicaId> eligibleReplicas;
    List<ReplicaId> offlineReplicas = new ArrayList<>();
    // This is a list of all replicas for this partition. This comes from replica information updated by us in helix
    // Data node configs via bootstrap tool and not external view. This would be same as ideal view.
    List<ReplicaId> allReplicas = new ArrayList<>(partitionId.getReplicaIds());

    boolean crossColoEnabled;
    switch (routerOperation) {
      case GetBlobOperation:
      case GetBlobInfoOperation:
        replicaSuccessTarget = routerConfig.routerGetSuccessTarget;
        replicaParallelism = routerConfig.routerGetRequestParallelism;
        crossColoEnabled = routerConfig.routerGetCrossDcEnabled;
        Map<ReplicaState, List<ReplicaId>> replicasByState = getReplicasByState(null,
            EnumSet.of(ReplicaState.BOOTSTRAP, ReplicaState.STANDBY, ReplicaState.LEADER, ReplicaState.INACTIVE,
                ReplicaState.OFFLINE));
        offlineReplicas = replicasByState.getOrDefault(ReplicaState.OFFLINE, new ArrayList<>());
        eligibleReplicas = new ArrayList<>();
        replicasByState.values().forEach(eligibleReplicas::addAll);
        // Whether to add offline replicas to replica pool is controlled by "routerOperationTrackerIncludeDownReplicas"
        // config. For now, we remove them from eligible replica list.
        eligibleReplicas.removeAll(offlineReplicas);
        break;
      case PutOperation:
        eligibleReplicas = getEligibleReplicas(datacenterName, EnumSet.of(ReplicaState.STANDBY, ReplicaState.LEADER));
        replicaSuccessTarget =
            routerConfig.routerGetEligibleReplicasByStateEnabled ? Math.max(eligibleReplicas.size() - 1,
                routerConfig.routerPutSuccessTarget) : routerConfig.routerPutSuccessTarget;
        replicaParallelism = routerConfig.routerGetEligibleReplicasByStateEnabled ? Math.min(eligibleReplicas.size(),
            routerConfig.routerPutRequestParallelism) : routerConfig.routerPutRequestParallelism;
        crossColoEnabled = false;
        break;
      case DeleteOperation:
        replicaSuccessTarget = routerConfig.routerDeleteSuccessTarget;
        replicaParallelism = routerConfig.routerDeleteRequestParallelism;
        crossColoEnabled = true;
        eligibleReplicas =
            getEligibleReplicas(null, EnumSet.of(ReplicaState.BOOTSTRAP, ReplicaState.STANDBY, ReplicaState.LEADER));
        break;
      case TtlUpdateOperation:
        replicaSuccessTarget = routerConfig.routerTtlUpdateSuccessTarget;
        replicaParallelism = routerConfig.routerTtlUpdateRequestParallelism;
        crossColoEnabled = true;
        eligibleReplicas =
            getEligibleReplicas(null, EnumSet.of(ReplicaState.BOOTSTRAP, ReplicaState.STANDBY, ReplicaState.LEADER));
        break;
      case UndeleteOperation:
        replicaParallelism = routerConfig.routerUndeleteRequestParallelism;
        crossColoEnabled = true;
        eligibleReplicas =
            getEligibleReplicas(null, EnumSet.of(ReplicaState.BOOTSTRAP, ReplicaState.STANDBY, ReplicaState.LEADER));
        // Undelete operation need to get global quorum. It will require a different criteria for success.
        // Here set the success target to the number of eligible replicas.
        replicaSuccessTarget = eligibleReplicas.size();
        break;
      case ReplicateBlobOperation:
        // Replicate one blob. Unlike PutBlob, crossColoEnabled is true.
        // ON_DEMAND_REPLICATION_TODO: may tune the order of the replica pool. Currently still local first and then remote.
        // a. right now we pick local replicas first for simplicity.
        // b. Among the remote replicas, we randomly pick one. We don't pick the replication leader.
        eligibleReplicas =
            getEligibleReplicas(null, EnumSet.of(ReplicaState.STANDBY, ReplicaState.LEADER, ReplicaState.BOOTSTRAP));
        replicaSuccessTarget = routerConfig.routerReplicateBlobSuccessTarget;
        replicaParallelism = routerConfig.routerReplicateBlobRequestParallelism;
        crossColoEnabled = true;
        break;
      default:
        throw new IllegalArgumentException("Unsupported operation: " + routerOperation);
    }
    if (replicaParallelism < 1) {
      throw new IllegalArgumentException(
          "Parallelism has to be > 0. Parallelism=" + replicaParallelism + ", routerOperation=" + routerOperation);
    }

    // Order the replicas so that local healthy replicas are ordered and returned first,
    // then the remote healthy ones, and finally the possibly down ones.
    List<? extends ReplicaId> replicas =
        routerConfig.routerGetEligibleReplicasByStateEnabled ? eligibleReplicas : partitionId.getReplicaIds();

    // In a case where a certain dc is decommissioned and blobs previously uploaded to this dc now have a unrecognizable
    // dc id. Current clustermap code will treat originating dc as null if dc id is not identifiable. To improve success
    // rate of cross-colo requests(GET/DELETE/TTLUpdate), operation tracker should be allowed to try remote dc with most
    // replicas first. This is useful in cluster with "unbalanced" replica distribution (i.e. 3 replicas in local dc and
    // 1 replica per remote dc)
    String reassignedOriginDc = null;
    if (originatingDcName == null && routerConfig.routerCrossColoRequestToDcWithMostReplicas) {
      Map<String, Long> dcToReplicaCnt = replicas.stream()
          .collect(Collectors.groupingBy(e -> e.getDataNodeId().getDatacenterName(), Collectors.counting()));
      List<Map.Entry<String, Long>> entryList = new ArrayList<>(dcToReplicaCnt.entrySet());
      entryList.sort(Map.Entry.comparingByValue());
      // we assign a dc with most replicas to "originatingDcName", which only takes effect when populating replica pool
      // (replicas in that colo have higher priority than other remote colos). Note that, "this.originatingDcName" still
      // keeps the actual originating dc name (which is null). This value forces operation tracker to go through replicas
      // in all dc(s) rather than terminating on not found in originating dc.
      reassignedOriginDc = entryList.get(entryList.size() - 1).getKey();
      logger.debug("Originating dc name is null and has been re-assigned to {}", reassignedOriginDc);
    }

    LinkedList<ReplicaId> backupReplicas = new LinkedList<>();
    LinkedList<ReplicaId> downReplicas = new LinkedList<>();
    if (shuffleReplicas) {
      Collections.shuffle(replicas);
    }

    // The priority here is local dc replicas, originating dc replicas, other dc replicas, down replicas.
    // To improve read-after-write performance across DC, we prefer to take local and originating replicas only,
    // which can be done by setting includeNonOriginatingDcReplicas False.
    List<ReplicaId> examinedReplicas = new ArrayList<>();
    originatingDcName = originatingDcName == null ? reassignedOriginDc : originatingDcName;
    for (ReplicaId replicaId : replicas) {
      examinedReplicas.add(replicaId);
      String replicaDcName = replicaId.getDataNodeId().getDatacenterName();
      boolean isLocalDcReplica = replicaDcName.equals(datacenterName);
      boolean isOriginatingDcReplica = replicaDcName.equals(originatingDcName);

      if (!replicaId.isDown()) {
        if (isLocalDcReplica) {
          addToBeginningOfPool(replicaId);
        } else if (crossColoEnabled && isOriginatingDcReplica) {
          addToEndOfPool(replicaId);
        } else if (crossColoEnabled) {
          backupReplicas.addFirst(replicaId);
        }
      } else {
        if (isLocalDcReplica) {
          downReplicas.addFirst(replicaId);
        } else if (crossColoEnabled) {
          downReplicas.addLast(replicaId);
        }
      }
    }
    List<ReplicaId> backupReplicasToCheck = new ArrayList<>(backupReplicas);
    List<ReplicaId> downReplicasToCheck = new ArrayList<>(downReplicas);

    // Add replicas that are neither in local dc nor in originating dc.
    backupReplicas.forEach(this::addToEndOfPool);

    if (routerConfig.routerOperationTrackerIncludeDownReplicas) {
      // Add those replicas deemed by native failure detector to be down
      downReplicas.forEach(this::addToEndOfPool);
      // Add those replicas deemed by Helix to be down (offline). This only applies to GET operation.
      // Adding this logic to mitigate situation where one or more Zookeeper clusters are suddenly unavailable while
      // ambry servers are still up.
      if (routerOperation == RouterOperation.GetBlobOperation
          || routerOperation == RouterOperation.GetBlobInfoOperation) {
        List<ReplicaId> remoteOfflineReplicas = new ArrayList<>();
        for (ReplicaId replica : offlineReplicas) {
          if (replica.getDataNodeId().getDatacenterName().equals(datacenterName)) {
            addToEndOfPool(replica);
          } else {
            remoteOfflineReplicas.add(replica);
          }
        }
        remoteOfflineReplicas.forEach(this::addToEndOfPool);
      }
    }
    totalReplicaCount = replicaPool.size();

    // MockPartitionId.getReplicaIds() is returning a shared reference which may cause race condition.
    // Please report the test failure if you run into this exception.
    Supplier<IllegalArgumentException> notEnoughReplicasException = () -> new IllegalArgumentException(
        generateErrorMessage(partitionId, examinedReplicas, replicaPool, backupReplicasToCheck, downReplicasToCheck,
            routerOperation));
    if (totalReplicaCount < getSuccessTarget()) {
      throw notEnoughReplicasException.get();
    }

    originatingDcTotalReplicaCount = (int) allReplicas.stream()
        .filter(replicaId -> replicaId.getDataNodeId().getDatacenterName().equals(this.originatingDcName))
        .count();
    originatingDcLeaderOrStandbyReplicas = new HashSet<>(
        getEligibleReplicas(this.originatingDcName, EnumSet.of(ReplicaState.STANDBY, ReplicaState.LEADER)));
    originatingDcNotFoundFailureThreshold =
        routerConfig.routerOperationTrackerCheckAllOriginatingReplicasForNotFound ? originatingDcTotalReplicaCount
            : Math.max(originatingDcTotalReplicaCount - routerConfig.routerPutSuccessTarget + 1, 0);

    this.otIterator = new OpTrackerIterator();
    logger.debug(
        "Router operation type: {}, successTarget = {}, parallelism = {}, originatingDcNotFoundFailureThreshold = {}, replicaPool = {}, originatingDC = {}",
        routerOperation, replicaSuccessTarget, replicaParallelism, originatingDcNotFoundFailureThreshold, replicaPool,
        originatingDcName);
  }

  /**
   * The dynamic success target is introduced mainly for following use case:
   * In the intermediate state of "move replica", when decommission of old replicas is initiated(but hasn't transited to
   * INACTIVE yet), the PUT requests should be rejected on old replicas. For frontends, they are seeing both old and new
   * replicas(lets say 3 old and 3 new) and the success target should be 6 - 1 = 5. In the aforementioned scenario, PUT
   * request failed on 3 old replicas. It seems we should fail whole PUT operation because number of remaining requests
   * is already less than success target.
   * From another point of view, however, PUT request is highly likely to succeed on 3 new replicas and we actually
   * could consider it success without generating "slip put" (which makes PUT latency worse). The reason is, if new PUTs
   * already succeeded on at least 2 new replicas,  read-after-write should always succeed because frontends are always
   * able to see new replicas and subsequent READ/DELETE/TtlUpdate request should succeed on at least 2 aforementioned
   * new replicas.
   */
  @Override
  public boolean hasSucceeded() {
    boolean hasSucceeded;
    if (routerOperation == RouterOperation.PutOperation && routerConfig.routerPutUseDynamicSuccessTarget) {
      // this logic only applies to replicas where the quorum can change during replica movement
      int dynamicSuccessTarget = Math.max(totalReplicaCount - disabledCount - 1, routerConfig.routerPutSuccessTarget);
      hasSucceeded = replicaSuccessCount >= dynamicSuccessTarget;
    } else {
      hasSucceeded = replicaSuccessCount >= replicaSuccessTarget;
    }
    return hasSucceeded;
  }

  @Override
  public boolean hasFailedOnNotFound() {
    if (routerOperation == RouterOperation.PutOperation) {
      return false;
    }

    if (originatingDcName == null || replicaSuccessCount > 0) {
      return false;
    }

    if (originatingDcNotFoundFailureThreshold > 0
        && originatingDcNotFoundCount >= originatingDcNotFoundFailureThreshold) {
      // If we get sufficient not found responses in originating DC, we can confirm that blob is not present in Ambry.
      logger.info("Terminating {} on {} due to Not_Found failure in originating DC {}, "
              + "NotFoundCount: {}, FailureThreshold: {}, TotalReplicaCount: {}, LeaderStandbyCount: {}, {}",
          routerOperation.name(), partitionId, originatingDcName, originatingDcNotFoundCount,
          originatingDcNotFoundFailureThreshold, originatingDcTotalReplicaCount,
          originatingDcLeaderOrStandbyReplicas.size(), getBlobIdLog());
      routerMetrics.failedOnTotalNotFoundCount.inc();
      return true;
    }

    return false;
  }

  @Override
  public boolean hasSomeUnavailability() {
    if (originatingDcName != null && originatingDcLeaderOrStandbyReplicas.size() < originatingDcTotalReplicaCount) {
      logger.info("{} on {} has some replicas unavailable in originating DC {}, TotalReplicaCount: {},"
              + "LeaderStandbyCount: {}, ReplicasByState: {}, {}", routerOperation.name(), partitionId, originatingDcName,
          originatingDcTotalReplicaCount, originatingDcLeaderOrStandbyReplicas.size(), allDcReplicasByState,
          getBlobIdLog());
      routerMetrics.failedMaybeDueToUnavailableReplicasCount.inc();
      return true;
    }
    return false;
  }

  @Override
  public boolean hasNotFound() {
    return totalNotFoundCount > 0;
  }

  @Override
  public List<ReplicaId> getSuccessReplica() {
    return successReplica;
  }

  @Override
  public int getSuccessCount() {
    return replicaSuccessCount;
  }

  @Override
  public boolean isDone() {
    return hasSucceeded() || hasFailed();
  }

  @Override
  public void onResponse(ReplicaId replicaId, TrackedRequestFinalState trackedRequestFinalState) {
    inflightCount--;
    // once a response has been received, a replica is no longer in the pool or currently in flight.
    modifyReplicasInPoolOrInFlightCount(-1);
    switch (trackedRequestFinalState) {
      case SUCCESS:
        successReplica.add(replicaId);
        replicaSuccessCount++;
        break;
      // Request disabled may happen when PUT/DELETE/TTLUpdate requests attempt to perform on replicas that are being
      // decommissioned (i.e STANDBY -> INACTIVE). This is because decommission may take some time and frontends still
      // hold old view. Aforementioned requests are rejected by server with Temporarily_Disabled error. For DELETE/TTLUpdate,
      // even though we may receive such errors, the success target is still same(=2). For PUT, we have to adjust the
      // success target (quorum) to let some PUT operations (with at least 2 requests succeeded on new replicas) succeed.
      // Currently, disabledCount only applies to PUT operation.
      case REQUEST_DISABLED:
        disabledCount++;
        break;
      default:
        failedCount++;
        // NOT_FOUND is a special error. When tracker sees >= numReplicasInOriginatingDc - 1 "NOT_FOUND" from the
        // originating DC, we can be sure the operation will end up with a NOT_FOUND error.
        if (trackedRequestFinalState == TrackedRequestFinalState.NOT_FOUND) {
          totalNotFoundCount++;
          if (replicaId.getDataNodeId().getDatacenterName().equals(originatingDcName)) {
            if (originatingDcLeaderOrStandbyReplicas.contains(replicaId)) {
              // Since bootstrap replicas could still be catching up, treat NotFound responses only from leader or
              // standby replicas as valid.
              originatingDcNotFoundCount++;
            }
          }
        }
    }
  }

  @Override
  public Iterator<ReplicaId> getReplicaIterator() {
    replicaIterator = replicaPool.iterator();
    return otIterator;
  }

  private class OpTrackerIterator implements Iterator<ReplicaId> {
    @Override
    public boolean hasNext() {
      return inflightCount < getCurrentParallelism() && replicaIterator.hasNext();
    }

    @Override
    public void remove() {
      replicaIterator.remove();
      inflightCount++;
    }

    @Override
    public ReplicaId next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      lastReturnedByIterator = replicaIterator.next();
      return lastReturnedByIterator;
    }
  }

  /**
   * Get eligible replicas by states for the specified data center. If dcName is null, it gets all eligible
   * replicas from all data centers.
   * @param dcName the name of data center from which the replicas should come from. This can be {@code null}.
   * @param states a set of {@link ReplicaState}(s) that replicas should match.
   * @return a list of eligible replicas that are in specified states.
   */
  private List<ReplicaId> getEligibleReplicas(String dcName, EnumSet<ReplicaState> states) {
    Map<ReplicaState, List<ReplicaId>> replicasByState = getReplicasByState(dcName, states);
    List<ReplicaId> eligibleReplicas = new ArrayList<>();
    for (List<ReplicaId> replicas : replicasByState.values()) {
      eligibleReplicas.addAll(replicas);
    }
    return eligibleReplicas;
  }

  /**
   * Get replicas in required states for the specified datacenter.
   * @param dcName the name of data center from which the replicas should come from. This can be {@code null}.
   * @param states a set of {@link ReplicaState}(s) that replicas should match.
   * @return a map whose key is {@link ReplicaState} and value is a list of {@link ReplicaId}(s) in that state.
   */
  Map<ReplicaState, List<ReplicaId>> getReplicasByState(String dcName, EnumSet<ReplicaState> states) {
    Map<ReplicaState, List<ReplicaId>> map = new HashMap<>();
    for (ReplicaState replicaState : states) {
      if (allDcReplicasByState.containsKey(replicaState)) {
        for (ReplicaId replicaId : allDcReplicasByState.get(replicaState)) {
          if (dcName == null || replicaId.getDataNodeId().getDatacenterName().equals(dcName)) {
            map.putIfAbsent(replicaState, new ArrayList<>());
            map.get(replicaState).add(replicaId);
          }
        }
      }
    }
    return map;
  }

  public boolean hasFailed() {
    if (routerOperation == RouterOperation.PutOperation && routerConfig.routerPutUseDynamicSuccessTarget) {
      return totalReplicaCount - failedCount < Math.max(totalReplicaCount - 1,
          routerConfig.routerPutSuccessTarget + disabledCount);
    } else {
      // If blob is not found in originating DC, we can terminate the operation early. Adding
      // routerConfig.routerOperationTrackerTerminateOnNotFoundEnabled as a safe guard to search replicas in all colos
      // before terminating the operation.
      if (routerConfig.routerOperationTrackerTerminateOnNotFoundEnabled && hasFailedOnNotFound()) {
        return true;
      }
      // if there is no possible way to use the remaining replicas to meet the success target,
      // deem the operation a failure.
      return replicaInPoolOrFlightCount + replicaSuccessCount < replicaSuccessTarget;
    }
  }

  /**
   * Exposed for testing only.
   * @return the number of replicas in current replica pool.
   */
  int getReplicaPoolSize() {
    return replicaPool.size();
  }

  /**
   * Add a replica to the beginning of the replica pool linked list.
   * @param replicaId the replica to add.
   */
  private void addToBeginningOfPool(ReplicaId replicaId) {
    modifyReplicasInPoolOrInFlightCount(1);
    replicaPool.addFirst(replicaId);
  }

  /**
   * Add a replica to the end of the replica pool linked list.
   * @param replicaId the replica to add.
   */
  private void addToEndOfPool(ReplicaId replicaId) {
    modifyReplicasInPoolOrInFlightCount(1);
    replicaPool.addLast(replicaId);
  }

  /**
   * Add {@code delta} to a replicas in pool or in flight counter.
   * @param delta the value to add to the counter.
   */
  private void modifyReplicasInPoolOrInFlightCount(int delta) {
    replicaInPoolOrFlightCount += delta;
  }

  /**
   * @return the success target number of this operation tracker for the provided replica type.
   */
  int getSuccessTarget() {
    return replicaSuccessTarget;
  }

  /**
   * This method determines the current number of parallel requests to send, based on the last request sent out or the
   * first replica in the pool if this is the first request sent.
   * @return the parallelism setting to honor.
   */
  int getCurrentParallelism() {
    return replicaParallelism;
  }

  /**
   * @return the number of requests that are temporarily disabled on certain replicas.
   */
  int getDisabledCount() {
    return disabledCount;
  }

  /**
   * @return current failed count in this tracker
   */
  int getFailedCount() {
    return failedCount;
  }

  /**
   * Returns string to for logging {@code BlobId}, if the blob id if not {@code null}.
   * @return string to for logging {@code BlobId}. Returns empty string if the blob id is {@code null}.
   */
  private final String getBlobIdLog() {
    return (blobId == null) ? "" : String.format(", BlobId: %s", blobId.toString());
  }

  /**
   * Helper function to catch a potential race condition in
   * {@link SimpleOperationTracker#SimpleOperationTracker(RouterConfig, RouterOperation, PartitionId, String, boolean, NonBlockingRouterMetrics)}.
   *  @param partitionId The partition on which the operation is performed.
   * @param examinedReplicas All replicas examined.
   * @param replicaPool Replicas added to replicaPool.
   * @param backupReplicas Replicas added to backupReplicas.
   * @param downReplicas Replicas added to downReplicas.
   * @param routerOperation The operation type associated with current operation tracker.
   */
  private static String generateErrorMessage(PartitionId partitionId, List<ReplicaId> examinedReplicas,
      List<ReplicaId> replicaPool, List<ReplicaId> backupReplicas, List<ReplicaId> downReplicas,
      RouterOperation routerOperation) {
    StringBuilder errMsg = new StringBuilder("Total Replica count ").append(replicaPool.size())
        .append(" is less than success target. ")
        .append("Router operation is ")
        .append(routerOperation)
        .append(". Partition is ")
        .append(partitionId)
        .append(", partition class is ")
        .append(partitionId.getPartitionClass())
        .append(" and associated resources are ")
        .append(partitionId.getResourceNames())
        .append(". examinedReplicas: ");
    for (ReplicaId replicaId : examinedReplicas) {
      errMsg.append(replicaId.getDataNodeId()).append(":").append(replicaId.isDown()).append(" ");
    }
    errMsg.append("replicaPool: ");
    for (ReplicaId replicaId : replicaPool) {
      errMsg.append(replicaId.getDataNodeId()).append(":").append(replicaId.isDown()).append(" ");
    }
    errMsg.append("backupReplicas: ");
    for (ReplicaId replicaId : backupReplicas) {
      errMsg.append(replicaId.getDataNodeId()).append(":").append(replicaId.isDown()).append(" ");
    }
    errMsg.append("downReplicas: ");
    for (ReplicaId replicaId : downReplicas) {
      errMsg.append(replicaId.getDataNodeId()).append(":").append(replicaId.isDown()).append(" ");
    }
    return errMsg.toString();
  }
}
