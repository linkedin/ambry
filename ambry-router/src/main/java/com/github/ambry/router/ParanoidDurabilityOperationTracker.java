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

    List<ReplicaId> allEligibleReplicas = getEligibleReplicas(null, EnumSet.of(ReplicaState.STANDBY, ReplicaState.LEADER));
    addReplicasToPool(allEligibleReplicas);
    this.otIterator = new ParanoidDurabilityTrackerIterator();
  }

  private void addReplicasToPool(List<? extends ReplicaId> replicas) {
    Collections.shuffle(replicas);

    for (ReplicaId replicaId : replicas) {
      addToPool(replicaId);
    }

    if(!enoughReplicas()) {
      throw new IllegalArgumentException(generateErrorMessage(partitionId));
    }
    initializeTracking();
    listRemoteReplicas();
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

  // Returns true if there are still remote replicas that have not been added to the final list, false otherwise.
  private boolean remoteReplicasLeft() {
    for(String dcName : remoteDatacenters) {
      if(!replicaPoolByDc.get(dcName).isEmpty()) {
        return true;
      }
    }
    return false;
  }

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

  private String generateErrorMessage(PartitionId partitionId) {
    StringBuilder errMsg = new StringBuilder("Partition " + partitionId + " has too few replicas to satisfy paranoid durability requirements. ");
    replicaPoolByDc.forEach((dcName, v) -> {
      errMsg.append("Datacenter: ").append(dcName).append(" has ").append(v.size()).append(" replicas in LEADER or STANDBY. ");
    });
    return errMsg.toString();
  }

  private boolean enoughReplicas() {
    if (localReplicas.size() < localReplicaSuccessTarget) {
      logger.error("Not enough replicas in local data center for partition " + partitionId
          + " to satisfy paranoid durability requirements " + "(wanted " + localReplicaSuccessTarget + ", but found " + localReplicas.size() + ")");
      return false;
    }

    if (remoteReplicas.size() < remoteReplicaSuccessTarget) {
      logger.error("Not enough replicas in remote data centers for partition " + partitionId
          + " to satisfy paranoid durability requirements " + "(wanted " + remoteReplicaSuccessTarget + ", but found " + remoteReplicas.size() + ")");
      return false;
    }
    return true;
  }

  public boolean hasSucceeded() {
    return hasSucceededLocally() && hasSucceededRemotely();
  }

  private boolean hasSucceededLocally() {
    // This logic only applies locally, to replicas where the quorum can change during replica movement.
    if(routerConfig.routerPutUseDynamicSuccessTarget) {
      int dynamicSuccessTarget = Math.max(totalLocalReplicaCount - disabledLocalReplicas - 1, routerConfig.routerPutSuccessTarget);
      return localReplicaSuccessCount >= dynamicSuccessTarget;
    }
    return localReplicaSuccessCount >= localReplicaSuccessTarget;
  }

  private boolean hasSucceededRemotely() {
    return remoteReplicaSuccessCount >= remoteReplicaSuccessTarget;
  }

  /**
   * Add a replica to the to the list for its data center.
   * @param replicaId the replica to add.
   */
  private void addToPool(ReplicaId replicaId) {
    String replicaDatacenterName = replicaId.getDataNodeId().getDatacenterName();
    modifyReplicasInPoolOrInFlightCount(replicaId, 1);
    if(replicaDatacenterName.equals(datacenterName)) {
      if(!replicaId.isDown())
        localReplicas.addFirst(replicaId);
    } else {
      localReplicas.addLast(replicaId);
    }

    // Remote replicas will be sorted later so just add them in any order.
    replicaPoolByDc.computeIfAbsent(replicaDatacenterName, k -> new LinkedList<>()).add(replicaId);
  }

  public void onResponse(ReplicaId replicaId, TrackedRequestFinalState trackedRequestFinalState) {
    super.onResponse(replicaId, trackedRequestFinalState);
    modifyReplicasInPoolOrInFlightCount(replicaId, -1);
    String dcName = replicaId.getDataNodeId().getDatacenterName();

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

  public boolean hasFailed() {
      return hasFailedLocally() || hasFailedRemotely();
  }

  private boolean hasFailedDynamically()  {
    if (routerConfig.routerPutUseDynamicSuccessTarget) {
      return (totalLocalReplicaCount - failedLocalReplicas) < Math.max(totalLocalReplicaCount - 1, localReplicaSuccessTarget + disabledLocalReplicas);
    }
    return false;
  }

  private boolean hasFailedRemotely() {
    return (remoteReplicaSuccessCount + remoteInflightCount + remoteReplicas.size()) < remoteReplicaSuccessTarget;
  }

  private boolean hasFailedLocally() {
    return hasFailedDynamically() || // The dynamic failure mode only applies in the local colo.
           ((localReplicaSuccessCount + localInflightCount + localReplicas.size()) < localReplicaSuccessTarget);
  }

  /**
   * Add {@code delta} to a replicas in pool or in flight counter.
   * @param delta the value to add to the counter.
   */
  private void modifyReplicasInPoolOrInFlightCount(ReplicaId replica, int delta) {
    String dcName = replica.getDataNodeId().getDatacenterName();
    if(dcName.equals(datacenterName)) {
      localInflightCount += delta;
    } else {
      remoteInflightCount += delta;
    }
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
        // hasNextRemote() must be true
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