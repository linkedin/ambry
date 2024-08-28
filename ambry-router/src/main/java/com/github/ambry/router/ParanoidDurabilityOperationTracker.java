/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.config.RouterConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link OperationTracker} for PUTs only (it does not support any other request types). It differs
 * from {@link SimpleOperationTracker} in that it is "paranoid" about durability. In addition to writing the new blob to
 * the required number of local replicas, it also requires writes to succeed for at least one replica in one remote
 * data center, effectively circumventing cross-datacenter replication (configurable). This is useful for clients such
 * as Samza, that require strong durability guarantees.
 *
 */
public class ParanoidDurabilityOperationTracker extends SimpleOperationTracker {
  private static final Logger logger = LoggerFactory.getLogger(ParanoidDurabilityOperationTracker.class);
  protected final int localReplicaSuccessTarget;
  protected int remoteReplicaSuccessTarget = 0;
  protected final int localReplicaParallelism;
  protected final int remoteReplicaParallelism;
  protected int localInflightCount = 0;
  protected int remoteInflightCount = 0;
  protected int localReplicaSuccessCount = 0;
  protected int remoteReplicaSuccessCount = 0;
  protected int remoteAttempts = 0;
  protected int remoteAttemptLimit = 2;
  private int currentDatacenterIndex = 0;
  int totalLocalReplicaCount;
  int failedLocalReplicas = 0;
  int disabledLocalReplicas = 0;
  private final Map<String, LinkedList<ReplicaId>> replicaPoolByDc = new HashMap<>();
  private final ParanoidDurabilityTrackerIterator otIterator;
  private final List<String> remoteDatacenters = new ArrayList<>();
  private final LinkedList<ReplicaId> localReplicas = new LinkedList<>();
  private final LinkedList<ReplicaId> remoteReplicas = new LinkedList<>();

  ParanoidDurabilityOperationTracker(RouterConfig routerConfig, PartitionId partitionId, String originatingDcName,
      NonBlockingRouterMetrics routerMetrics) {
    super(routerConfig, RouterOperation.PutOperation, partitionId, originatingDcName, true, routerMetrics);

    localReplicaParallelism = routerConfig.routerPutRequestParallelism;
    remoteReplicaParallelism = routerConfig.routerPutRemoteRequestParallelism;
    localReplicaSuccessTarget = routerConfig.routerPutSuccessTarget;
    remoteReplicaSuccessTarget = routerConfig.routerPutRemoteSuccessTarget;
    remoteAttemptLimit = routerConfig.routerPutRemoteAttemptLimit;

    // Calling getEligibleReplicas with null will return replicas from all data centers.
    List<ReplicaId> allEligibleReplicas = getEligibleReplicas(null, EnumSet.of(ReplicaState.STANDBY, ReplicaState.LEADER));
    addReplicasToPool(allEligibleReplicas);
    this.otIterator = new ParanoidDurabilityTrackerIterator();
  }

  /**
   * Create a "pool" of replicas that we can send requests to. Replicas are divided up into local and remote replicas.
   * The remote replicas are sorted to obtain the best possible performance (given the additional latency and bandwidth
   * associated with cross-colo requests).
   * @param replicas the list of replicas to add to the pool.
   */
  private void addReplicasToPool(List<? extends ReplicaId> replicas) {
    Collections.shuffle(replicas);

    for (ReplicaId replicaId : replicas) {
      addToPool(replicaId);
    }

    initializeTracking();
    listRemoteReplicas();
    if(!enoughReplicas()) {
      throw new IllegalArgumentException(generateErrorMessage(partitionId));
    }
  }

  /**
   * Generate the list of remote replicas to send requests to. The list is ordered by Helix state as follows:
   * 1. ReplicaIds in Helix LEADER state. Since Ambry replication happens between leaders in data centers, we prefer to
   *    prioritize sending requests to leaders first, in order to reduce replication traffic (if a blob is already
   *    present on a leader, we do not need to request it from another data center).
   * 2. ReplicaIds in Helix STANDBY state.
   * 3. All other ReplicaIds.
   * Note that the list of remote replicas is also ordered by alternating remote data centers. The intuition is that if
   * the first request to a remote data center fails, we attempt reaching a replica in a different data center instead
   * of retrying the same data center. A hypothetical example of such an ordered list, assuming that the remote colos
   * are prod-ltx1 and prod-lva1:
   * [prod-ltx1-LEADER, prod-lva1-LEADER, prod-ltx1-STANDBY, prod-lva1-STANDBY, prod-ltx1-STANDBY, prod-lva1-STANDBY]
   */
  private void listRemoteReplicas() {
    // Custom comparator to sort ReplicaId instances, such that those that are in Helix LEADER state appear first,
    // followed by STANDBY, followed by anything else.
    Comparator<ReplicaId> replicaIdComparator = (r1, r2) -> {
      if (getReplicaState(r1) == ReplicaState.LEADER && getReplicaState(r2) != ReplicaState.LEADER) {
        return -1;
      } else if (getReplicaState(r1) != ReplicaState.LEADER && getReplicaState(r2) == ReplicaState.LEADER) {
        return 1;
      } else if (getReplicaState(r1) == ReplicaState.STANDBY && getReplicaState(r2) != ReplicaState.STANDBY) {
        return -1;
      } else if (getReplicaState(r1) != ReplicaState.STANDBY && getReplicaState(r2) == ReplicaState.STANDBY) {
        return 1;
      }
      return 0;
    };

    // Sort the replicas in each remote data center such that LEADER replicas appear first, then STANDBY replicas.
    for(String dcName : remoteDatacenters) {
      Collections.sort(replicaPoolByDc.get(dcName), replicaIdComparator);
    }

    // While there are still replicas left in any remote colo, append remote replicas to the list in alternating colo
    // order.
    while(remoteReplicasLeft())
    {
      String nextDc = getNextRemoteDatacenter();
      if(!replicaPoolByDc.get(nextDc).isEmpty())
        remoteReplicas.addLast(replicaPoolByDc.get(nextDc).removeFirst());
    }
  }

  /**
   * Returns true if there are still remote replicas that have not yet been added to the final list (i.e. remoteReplicas),
   * false otherwise.
   */
  private boolean remoteReplicasLeft() {
    for(String dcName : remoteDatacenters) {
      if(!replicaPoolByDc.get(dcName).isEmpty()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns the "next" remote data center that we should pick up a replica from, for insertion into the remoteReplicas
   * list. Remote data center names are returned in a round-robin fashion.
   * @return the String name of the next colo.
   */
  private String getNextRemoteDatacenter()  {
    int i = 0;
    int remoteDatacenterCount = remoteDatacenters.size();
    String nextDatacenter;
    do {
      nextDatacenter = remoteDatacenters.get(currentDatacenterIndex);
      currentDatacenterIndex = (currentDatacenterIndex + 1) % remoteDatacenterCount;
      i++;
    } while (replicaPoolByDc.get(nextDatacenter).isEmpty() &&
        i <= remoteDatacenterCount);                           // Guard against infinite loop
    return nextDatacenter;
  }

  /**
   * Initialize various tracking variables that are used to determine the success or failure of this PUT operation.
   */
  private void initializeTracking() {
    totalLocalReplicaCount = localReplicas.size();
    failedLocalReplicas = 0;
    disabledLocalReplicas = 0;

    replicaPoolByDc.forEach((dcName, v) -> {
      if(!dcName.equals(datacenterName)) {
        remoteDatacenters.add(dcName);
      }
    });
  }

  /**
   * Generate a human-readable error message for when there are not enough replicas to satisfy the request.
   * @param partitionId The chosen partition.
   * @return
   */
  private String generateErrorMessage(PartitionId partitionId) {
    StringBuilder errMsg = new StringBuilder("Partition " + partitionId + " has too few replicas to satisfy paranoid durability requirements. ");
    replicaPoolByDc.forEach((dcName, v) -> {
      errMsg.append("Datacenter: ").append(dcName).append(" has ").append(v.size()).append(" replicas in LEADER or STANDBY. ");
    });
    return errMsg.toString();
  }

  /**
   * Check if there are enough replicas to satisfy the paranoid durability requirements.
   * @return true if there are enough replicas, false otherwise.
   */
  private boolean enoughReplicas() {
    if (localReplicas.size() < localReplicaSuccessTarget) {
      logger.error("Not enough replicas in local data center for partition " + partitionId
          + " to satisfy paranoid durability requirements " + "(wanted " + localReplicaSuccessTarget + ", but found " + localReplicas.size() + ")");
      routerMetrics.paranoidDurabilityNotEnoughReplicasCount.inc();
      return false;
    }

    if (remoteReplicas.size() < remoteReplicaSuccessTarget) {
      logger.error("Not enough replicas in remote data centers for partition " + partitionId
          + " to satisfy paranoid durability requirements " + "(wanted " + remoteReplicaSuccessTarget + ", but found " + remoteReplicas.size() + ")");
      routerMetrics.paranoidDurabilityNotEnoughReplicasCount.inc();
      return false;
    }
    return true;
  }

  /**
   * Check if the operation has succeeded. This is determined by the number of successful responses received from both
   * local and remote replicas.
   * @return true if we have received enough successful responses from both local and remote replicas, false otherwise.
   */
  public boolean hasSucceeded() {
    if (hasSucceededLocally() && hasSucceededRemotely()) {
      routerMetrics.paranoidDurabilitySuccessCount.inc();
      return true;
    }
    return false;
  }

  /**
   * Check if the operation has succeeded locally (i.e. in the local data center).
   * @return true if we have received enough successful responses from local replicas, false otherwise.
   */
  private boolean hasSucceededLocally() {
    // This logic only applies locally, to replicas where the quorum can change during replica movement.
    if(routerConfig.routerPutUseDynamicSuccessTarget) {
      int dynamicSuccessTarget = Math.max(totalLocalReplicaCount - disabledLocalReplicas - 1, routerConfig.routerPutSuccessTarget);
      return localReplicaSuccessCount >= dynamicSuccessTarget;
    }
    return localReplicaSuccessCount >= localReplicaSuccessTarget;
  }

  /**
   * Check if the operation has succeeded remotely (i.e. in the remote data centers).
   * @return true if we have received enough successful responses from remote replicas, false otherwise.
   */
  private boolean hasSucceededRemotely() {
    if (remoteReplicaSuccessCount >= remoteReplicaSuccessTarget) {
      return true;
    } else if(remoteAttempts >= remoteAttemptLimit) {
      // We want to limit the number of retries that we issue to remote colos. At times of excessive network latency,
      // we may end up issuing a large number of retries to remote colos. This is undesirable, since it will lead to
      // excessive direct memory usage, because ambry-frontend will hold the state for every request until it is completed.
      logger.error("Paranoid durability PUT failed in remote data centers for partition "
          + partitionId + " - gave up after " + remoteAttempts + " attempts. If the local writes succeeded, "
          + "we will still return success to the client. This is likely a network issue.");
      routerMetrics.paranoidDurabilityRemoteRetriesExceededCount.inc();
      return true;
    }
    return false;
  }

  /**
   * Add a replica to the to the list for its data center.
   * @param replicaId the replica to add.
   */
  private void addToPool(ReplicaId replicaId) {
    String replicaDatacenterName = replicaId.getDataNodeId().getDatacenterName();
    if (replicaDatacenterName.equals(datacenterName)) {
      if (!replicaId.isDown()) {
        localReplicas.addFirst(replicaId);
      } else {
        localReplicas.addLast(replicaId);  // We may still attempt to send requests to down replicas as a very last resort.
      }
    } else {
      // Remote replicas will be sorted later so just add them in any order.
      replicaPoolByDc.computeIfAbsent(replicaDatacenterName, k -> new LinkedList<>()).add(replicaId);
    }
  }

  /**
   * Handle a response from a replica, updating the state of the operation.
   * @param replicaId ReplicaId associated with this response.
   * @param trackedRequestFinalState The final state of a single request being tracked.
   */
  public void onResponse(ReplicaId replicaId, TrackedRequestFinalState trackedRequestFinalState) {
    super.onResponse(replicaId, trackedRequestFinalState);
    String dcName = replicaId.getDataNodeId().getDatacenterName();

    if(dcName.equals(datacenterName)) {
      localInflightCount--;
    } else {
      remoteInflightCount--;
      remoteAttempts++;
    }

    switch (trackedRequestFinalState) {
      case SUCCESS:
        if (dcName.equals(datacenterName)) {
          localReplicaSuccessCount++;
        } else {
          remoteReplicaSuccessCount++;
        }
        break;
      case REQUEST_DISABLED:
        if (dcName.equals(datacenterName)) { // Only track this locally, for dynamic success/failure purposes
          disabledLocalReplicas++;
        }
        break;
      default:
        if (dcName.equals(datacenterName)) { // Only track this locally, for dynamic success/failure purposes
          failedLocalReplicas++;
        }
    }
  }

  /**
   * Check if the operation has failed. This is determined by the number of failed responses received from both local and
   * remote replicas.
   * @return true if we have received enough failed responses from both local and remote replicas, false otherwise.
   */
  public boolean hasFailed() {
    return hasFailedLocally() || hasFailedRemotely();
  }

  /**
   * Check if the operation has failed "dynamically", accounting for the intermediate Helix state where we may have more
   * replicas than normal (e.g. Helix brought up a replacement replica, but the old one is still online).
   * @return true if we have received enough failed responses from local replicas to trigger dynamic failure, false
   * otherwise.
   */
  private boolean hasFailedDynamically()  {
    if (routerConfig.routerPutUseDynamicSuccessTarget) {
      return (totalLocalReplicaCount - failedLocalReplicas) < Math.max(totalLocalReplicaCount - 1, localReplicaSuccessTarget + disabledLocalReplicas);
    }
    return false;
  }

  /**
   * Check if the operation has failed remotely (i.e. in the remote data centers).
   * @return true if we have received enough failed responses from remote replicas that the operation cannot possibly
   * succeed, false otherwise.
   */
  private boolean hasFailedRemotely() {
    // If we have exceeded the number of attempts to remote colos, we should give up and return success to the client.
    // This situation may occur at times of excessive network latency, where we may end up issuing a large number of
    // retries to remote colos. This is undesirable, since it will lead to excessive direct memory usage (incident-1455).
    if (remoteAttempts >= remoteAttemptLimit) {
      return false;
    } else if ((remoteReplicaSuccessCount + remoteInflightCount + remoteReplicas.size()) < remoteReplicaSuccessTarget) {
      logger.error("Paranoid durability PUT failed in remote data centers for partition " + partitionId);
      routerMetrics.paranoidDurabilityFailureCount.inc();
      return true;
    }
    return false;
  }

  /**
   * Check if the operation has failed locally (i.e. in the local data center).
   * @return true if we have received enough failed responses from local replicas that the operation cannot possibly
   * succeed, false otherwise.
   */
  private boolean hasFailedLocally() {
    if (hasFailedDynamically() || // The dynamic failure mode only applies in the local colo.
        ((localReplicaSuccessCount + localInflightCount + localReplicas.size()) < localReplicaSuccessTarget)) {
      logger.error("Paranoid durability PUT failed in local data center (" + datacenterName + ") for partition " + partitionId);
      routerMetrics.paranoidDurabilityFailureCount.inc();
      return true;
    }
    return false;
  }


  int getCurrentLocalParallelism() {
    return replicaParallelism;
  }

  int getCurrentRemoteParallelism() {
    return remoteReplicaParallelism;
  }

  @Override
  public Iterator<ReplicaId> getReplicaIterator() {
    return otIterator;
  }

  // This class is used to iterate over the replicas that have been chosen for this PUT operation.
  // Traditionally, ambry-frontend has used the Java Iterator abstraction for this purpose, even though the underlying
  // implementation here and in the other OperationTracker classes is not a traditional Iterator. This is because we
  // keep track of in-flight requests, successful and failed responses, and the configured parallelism and success targets.
  // Sometimes hasNext() will return false, even though there are more replicas we could send requests to, because we
  // need to wait for more responses to come back before we can send more requests. Thus, the caller needs to "poll"
  // hasNext() until it returns true before calling next().
  //
  // Bear in mind the following about how ParanoidDurabilityTrackerIterator behaves:
  // 1. We send requests to local replicas until we have fulfilled local replica parallelism. CONCURRENTLY, we also
  //    allow sending requests to remote replicas until we have fulfilled remote replica parallelism. We do not want to
  //    wait for PUT requests to succeed locally before attempting the remote writes. PUT requests are usually very
  //    reliable in Ambry anyway, since we filter out only the healthiest replicas to send requests to, so we would
  //    expect very few failed requests.
  // 2. The order in which we send requests to remote replicas is determined by the order in which the remote replicas
  //    are listed in the remoteReplicas list, which is generated by the listRemoteReplicas() method.
  private class ParanoidDurabilityTrackerIterator implements Iterator<ReplicaId> {
    @Override
    public boolean hasNext() {
      return hasNextLocal() || hasNextRemote();
    }

    @Override
    public void remove() {
      if(isLocalReplica(lastReturnedByIterator)){
        localInflightCount++;
        localReplicas.remove(lastReturnedByIterator);
      } else {
        remoteInflightCount++;
        remoteReplicas.remove(lastReturnedByIterator);
      }
    }

    @Override
    public ReplicaId next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      if (hasNextLocal()) {
        lastReturnedByIterator = localReplicas.removeFirst();
      } else {
        // At this point, hasNextRemote() must be true
        lastReturnedByIterator = remoteReplicas.removeFirst();
      }
      return lastReturnedByIterator;
    }

    private boolean hasNextLocal() {
      return localInflightCount < getCurrentLocalParallelism() && !localReplicas.isEmpty();
    }

    private boolean hasNextRemote() {
      return remoteInflightCount < getCurrentRemoteParallelism() && !remoteReplicas.isEmpty();
    }

    private boolean isLocalReplica(ReplicaId replica) {
      return replica.getDataNodeId().getDatacenterName().equals(datacenterName);
    }
  }
}