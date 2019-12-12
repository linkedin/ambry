/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.clustermap;

/**
 * A class helps check if replicas have synced up.
 * There are two use cases:
 *     1. determine if new added replica has caught up with peers (occurs within BOOTSTRAP -> STANDBY transition)
 *     2. determine peer replicas have caught up with old replica that is being decommissioned; (occurs in STANDBY ->
 *     INACTIVE and INACTIVE -> OFFLINE transitions)
 *
 * To track state of replica that is catching up or being commissioned, this class leverages a {@link java.util.concurrent.CountDownLatch}.
 * Every time {@link ReplicaSyncUpManager#initiateBootstrap(ReplicaId)} is called, a new latch (with initial value = 1)
 * is created associated with given replica. Any caller that invokes {@link ReplicaSyncUpManager#waitBootstrapCompleted(String)}
 * is blocked and wait until corresponding latch counts to zero. External component (i.e. replication manager) is able to
 * call {@link ReplicaSyncUpManager#onBootstrapComplete(String)} or {@link ReplicaSyncUpManager#onBootstrapError(String)}
 * to mark sync-up success or failure by counting down the latch. This will unblock caller waiting fot this latch and
 * proceed with subsequent actions.
 */
public interface ReplicaSyncUpManager {

  /**
   * Initiate bootstrap process if the replica is newly added and needs to catch up with peer ones.
   * @param replicaId the replica to bootstrap
   */
  void initiateBootstrap(ReplicaId replicaId);

  /**
   * Wait until bootstrap for given replica is complete. The method is blocked on a {@link java.util.concurrent.CountDownLatch}
   * until given replica has caught up with enough peer replicas either in local DC or remote DCs
   * @param partitionName partition name of replica that in bootstrap state
   * @throws InterruptedException
   */
  void waitBootstrapCompleted(String partitionName) throws InterruptedException;

  /**
   * Update replica lag (in byte) between two replicas (chaser and precursor)
   * @param source the replica which is catching up with target replica
   * @param target the replica which is in leading position
   * @param lagInBytes replica lag bytes
   * @return whether the lag is updated or not. If {@code false}, it means the chaser is not tracked in this service.
   *         Either the replica has caught up and removed from service or it is an existing replica that doesn't need catchup.
   */
  boolean updateLagBetweenReplicas(ReplicaId source, ReplicaId target, long lagInBytes);

  /**
   * Whether given replica has synced up with its peers.
   * @param replicaId replica to check
   * @return {@code true} if given replica has caught up with peers or peer replicas have synced up with given replica
   *         (this occurs when given replica is being decommissioned)
   */
  boolean isSyncUpComplete(ReplicaId replicaId);

  /**
   * Bootstrap on given replica is complete. This method will count down the latch associated with given replica and
   * unblock external service waiting on this latch.
   * @param partitionName partition name of replica on which bootstrap completes.
   */
  void onBootstrapComplete(String partitionName);

  /**
   * When exception/error occurs during bootstrap. This method will count down latch and terminates bootstrap.
   * @param partitionName partition name of replica which encounters error.
   */
  void onBootstrapError(String partitionName);

  // TODO introduce decommission logic in sync-up service. For example, initiateDecommission(String partitionName)
}
