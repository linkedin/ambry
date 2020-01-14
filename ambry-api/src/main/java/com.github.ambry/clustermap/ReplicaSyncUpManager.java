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
 */
public interface ReplicaSyncUpManager {

  /**
   * Initiate bootstrap process if the replica is newly added and needs to catch up with peer ones.
   * @param replicaId the replica to bootstrap
   */
  void initiateBootstrap(ReplicaId replicaId);

  /**
   * Wait until bootstrap on given replica is complete.
   * until given replica has caught up with enough peer replicas either in local DC or remote DCs
   * @param partitionName partition name of replica that in bootstrap state
   * @throws InterruptedException
   */
  void waitBootstrapCompleted(String partitionName) throws InterruptedException;

  /**
   * Update replica lag (in byte) between two replicas (local and peer replica)
   * @param localReplica the replica that resides on current node
   * @param peerReplica the peer replica of local one.
   * @param lagInBytes replica lag bytes
   * @return whether the lag is updated or not. If {@code false}, it means the local replica is not tracked in this service.
   *         Either the replica has caught up and removed from service or it is an existing replica that doesn't need catchup.
   */
  boolean updateLagBetweenReplicas(ReplicaId localReplica, ReplicaId peerReplica, long lagInBytes);

  /**
   * Whether given replica has synced up with its peers.
   * @param replicaId replica to check
   * @return {@code true} if given replica has caught up with peers or peer replicas have synced up with given replica
   *         (this occurs when given replica is being decommissioned)
   */
  boolean isSyncUpComplete(ReplicaId replicaId);

  /**
   * Bootstrap on given replica is complete.
   * @param replicaId the replica which completes bootstrap.
   */
  void onBootstrapComplete(ReplicaId replicaId);

  /**
   * Deactivation on given replica is complete.
   * @param replicaId the replica which completes deactivation.
   */
  void onDeactivationComplete(ReplicaId replicaId);

  /**
   * When exception/error occurs during bootstrap.
   * @param replicaId the replica which encounters error.
   */
  void onBootstrapError(ReplicaId replicaId);

  /**
   * When exception/error occurs during deactivation.
   * @param replicaId the replica which encounters error.
   */
  void onDeactivationError(ReplicaId replicaId);

  /**
   * Initiate deactivation process if the replica should become INACTIVE from STANDBY on current node.
   * @param replicaId the replica to deactivate
   */
  void initiateDeactivation(ReplicaId replicaId);

  /**
   * Wait until deactivation on given replica is complete.
   * @param partitionName the name of replica that is within Standby-To-Inactive transition.
   * @throws InterruptedException
   */
  void waitDeactivationCompleted(String partitionName) throws InterruptedException;

  /**
   * Initiate disconnection process to stop replica and make it offline. This happens when replica is going to be
   * removed from current node. For Ambry, it disconnection occurs in Inactive-To-Offline transition.
   * @param replicaId the {@link ReplicaId} to be disconnected.
   */
  void initiateDisconnection(ReplicaId replicaId);

  /**
   * Wait until disconnection on given partition is complete.
   * @param partitionName partition name of replica that in disconnection process
   */
  void waitDisconnectionCompleted(String partitionName) throws InterruptedException;

  /**
   * When disconnection completes on given replica and then it becomes offline.
   * @param replicaId the {@link ReplicaId} on which disconnection completes
   */
  void onDisconnectionComplete(ReplicaId replicaId);

  /**
   * When exception/error occurs during disconnection.
   * @param replicaId the {@link ReplicaId} which encounters error.
   */
  void onDisconnectionError(ReplicaId replicaId);
}
