/**
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.config.RouterConfig;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * An implementation of {@link OperationTracker}. It internally maintains the status of a corresponding operation, and
 * returns information that decides if the operation should continue or terminate.
 *
 * This OperationTracker only works for {@link UndeleteOperation}. Since an {@link UndeleteOperation} requires a global
 * quorum for success, a map of datacenter to number of success requests is used to keep track of how many success requests
 * tracker has seen in each datacenter. Once it reaches quorum in all the datacenter, it returns true for success. Or if
 * any datacenter see failure requests reach the quorum, it return true for failure.
 */
public class UndeleteOperationTracker extends SimpleOperationTracker {
  private final Map<String, Integer> numReplicasInDcs = new HashMap<>();
  private final Map<String, Integer> numSuccessInDcs = new HashMap<>();
  private final Map<String, Integer> numFailureInDcs = new HashMap<>();

  /**
   * Constructs an {@link UndeleteOperationTracker}
   * @param routerConfig The {@link RouterConfig} containing the configs for operation tracker.
   * @param partitionId The partition on which the operation is performed.
   * @param originatingDcName name of originating DC whose replicas should be tried first.
   */
  UndeleteOperationTracker(RouterConfig routerConfig, PartitionId partitionId, String originatingDcName) {
    super(routerConfig, RouterOperation.UndeleteOperation, partitionId, originatingDcName, false);
    List<? extends ReplicaId> replicas = partitionId.getReplicaIds();
    for (ReplicaId replica : replicas) {
      String dcName = replica.getDataNodeId().getDatacenterName();
      numReplicasInDcs.put(dcName, numReplicasInDcs.getOrDefault(dcName, 0) + 1);
    }
    Map<String, Integer> numEligibleReplicasInDcs = new HashMap<>();
    for (ReplicaId replica : replicaPool) {
      String dcName = replica.getDataNodeId().getDatacenterName();
      numEligibleReplicasInDcs.put(dcName, numEligibleReplicasInDcs.getOrDefault(dcName, 0) + 1);
    }

    if (!hasReachedGlobalQuorum(numReplicasInDcs, numEligibleReplicasInDcs)) {
      throw new IllegalArgumentException(
          "Eligible replicas are not sufficient for undelete operation for partition " + partitionId);
    }
    // Consider not-eligible hosts as failure
    for (String dcName : numReplicasInDcs.keySet()) {
      int totalNum = numReplicasInDcs.get(dcName);
      int eligibleNum = numEligibleReplicasInDcs.getOrDefault(dcName, 0);
      numFailureInDcs.put(dcName, totalNum - eligibleNum);
    }
  }

  @Override
  public void onResponse(ReplicaId replicaId, TrackedRequestFinalState trackedRequestFinalState) {
    super.onResponse(replicaId, trackedRequestFinalState);
    String dcName = replicaId.getDataNodeId().getDatacenterName();
    if (trackedRequestFinalState == TrackedRequestFinalState.SUCCESS) {
      numSuccessInDcs.put(dcName, numSuccessInDcs.getOrDefault(dcName, 0) + 1);
    } else {
      numFailureInDcs.put(dcName, numFailureInDcs.getOrDefault(dcName, 0) + 1);
    }
  }

  @Override
  public boolean hasSucceeded() {
    return hasReachedGlobalQuorum(numReplicasInDcs, numSuccessInDcs);
  }

  @Override
  public boolean hasFailed() {
    return hasReachedAnyLocalQuorum(numReplicasInDcs, numFailureInDcs);
  }

  /**
   * Return true if the {@code currentNumberInDcs}'s each value has reached the quorum of corresponding value
   * in {@code totalNumberInDcs}.
   * @param totalNumberInDcs The total number of replicas in each datacenter.
   * @param currentNumberInDcs The current number of replicas in each datacenter.
   * @return true if current numbers reach the global quorum.
   */
  static boolean hasReachedGlobalQuorum(Map<String, Integer> totalNumberInDcs,
      Map<String, Integer> currentNumberInDcs) {
    boolean hasReached = true;
    if (totalNumberInDcs.size() == currentNumberInDcs.size()) {
      for (String dcName : totalNumberInDcs.keySet()) {
        int totalNum = totalNumberInDcs.get(dcName);
        int currentNum = currentNumberInDcs.getOrDefault(dcName, 0);
        if (currentNum <= totalNum / 2) {
          hasReached = false;
          break;
        }
      }
    } else {
      hasReached = false;
    }
    return hasReached;
  }

  /**
   * Return true if any of the {@code currentNumberInDcs}'s value has reached the quorum of corresponding value
   * in {@code totalNumberInDcs}.
   * @param totalNumberInDcs The total number of replicas in each datacenter.
   * @param currentNumberInDcs The current number of replicas in each datacenter.
   * @return true if any number reaches the quorum.
   */
  static boolean hasReachedAnyLocalQuorum(Map<String, Integer> totalNumberInDcs,
      Map<String, Integer> currentNumberInDcs) {
    for (String dcName : totalNumberInDcs.keySet()) {
      int totalNum = totalNumberInDcs.get(dcName);
      if (currentNumberInDcs.containsKey(dcName)) {
        int currentNum = currentNumberInDcs.get(dcName);
        if (currentNum > totalNum / 2) {
          return true;
        }
      }
    }
    return false;
  }
}
