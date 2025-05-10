/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.accountstats.AccountStatsStore;
import com.github.ambry.commons.Callback;
import com.github.ambry.server.AmbryStatsReport;
import com.github.ambry.server.storagestats.AggregatedAccountStorageStats;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;


/**
 * A ClusterParticipant is a component that makes up the Ambry cluster.
 */
public interface ClusterParticipant extends AutoCloseable {

  /**
   * Initiate the participation of cluster participant.
   * @throws IOException
   */
  void participate() throws IOException;

  /**
   * Set the sealed state of the given replica.
   * @param replicaId         the {@link ReplicaId} whose sealed state will be updated.
   * @param replicaSealStatus {@link ReplicaSealStatus} to be set.
   * @return {@code true} if set replica sealed state was successful. {@code false} if not.
   */
  boolean setReplicaSealedState(ReplicaId replicaId, ReplicaSealStatus replicaSealStatus);

  /**
   * Set or reset the stopped state of the given replica.
   * @param replicaIds a list of replicas whose stopped state will be updated
   * @param markStop if true, the replica will be marked as stopped; otherwise it will be marked as started.
   * @return {@code true} if set replica stopped state was successful. {@code false} if not.
   */
  boolean setReplicaStoppedState(List<ReplicaId> replicaIds, boolean markStop);

  /**
   * Set or reset the disabled state of the given replica
   * @param replicaId the {@link ReplicaId} whose disabled state will be updated.
   */
  default void setReplicaDisabledState(ReplicaId replicaId, boolean disable) {
  }

  /**
   * Get a list of replicas that are marked as sealed (read-only).
   * @return a list of all sealed replicas.
   */
  List<String> getSealedReplicas();

  /**
   * Get a list of replicas that are marked as partially sealed.
   * @return a list of all partially sealed replicas.
   */
  List<String> getPartiallySealedReplicas();

  /**
   * Get a list of replicas that are marked as stopped.
   * @return a list of all stopped replicas.
   */
  List<String> getStoppedReplicas();

  /**
   * Get a list of replicas that have been disabled.
   * @return a list of all disabled replicas.
   */
  default List<String> getDisabledReplicas() {
    return Collections.emptyList();
  }

  /**
   * Register a listener for leadership changes in partitions of this node.
   * @param listenerType the type of listener, which is defined in {@link StateModelListenerType}
   * @param partitionStateChangeListener listener to register.
   */
  void registerPartitionStateChangeListener(StateModelListenerType listenerType,
      PartitionStateChangeListener partitionStateChangeListener);

  /**
   * Gets the {@link ReplicaSyncUpManager} object.
   * @return {@link ReplicaSyncUpManager} that is used to determine new replica has caught up with peers or peer replicas
   * have caught up with old replica that is being decommissioned.
   */
  ReplicaSyncUpManager getReplicaSyncUpManager();

  /**
   * Update disk/replica infos associated with current data node in cluster (this occurs when replica addition/removal
   * on current node is complete and local changes will be broadcast to all listeners in this cluster)
   * @param replicaId the {@link ReplicaId} whose info should be updated on current node
   * @param shouldExist Whether the replica info should exist or not. When {@code true}, replica info will be added if
   *                      it is missing in current node info. When {@code false}, replica info will be removed if present.
   * @return if {@code true}, node info is successfully updated. {@code false} otherwise.
   */
  boolean updateDataNodeInfoInCluster(ReplicaId replicaId, boolean shouldExist);

  default boolean removeReplicasFromDataNode(List<ReplicaId> replicaIds) {
    return true;
  }

  /**
   * Set initial local partitions that the cluster participant hosts.
   * @param localPartitions a collection of initial local partitions.
   */
  default void setInitialLocalPartitions(Collection<String> localPartitions) {
  }

  /**
   * Reset given partition to initial state.
   * @param partitionName the partition to reset.
   * @return whether reset operation succeeded or not.
   */
  default boolean resetPartitionState(String partitionName) {
    return true;
  }

  /**
   * Reset given partitions to initial state. These partitions have to be in ERROR state for this reset to work.
   * @param partitionNames A list of partition names.
   * @return True if the reset is a success
   */
  default boolean resetPartitionState(List<String> partitionNames) {
    return true;
  }

  /**
   * @return a map of registered state change listeners (if there are any) in this cluster participant.
   */
  Map<StateModelListenerType, PartitionStateChangeListener> getPartitionStateChangeListeners();

  /**
   * @return {@code true} if this participant supports dynamic partition state changes.
   */
  default boolean supportsStateChanges() {
    return false;
  }

  /**
   * Terminate the participant.
   */
  @Override
  void close();

  /**
   * Returning a {@link DistributedLock} for the given resource with the given message.
   * @param resource The resource on which to create a distributed lock. Locks created for different resource
   *                 should be independent of each other.
   * @param message The message for this lock creation
   * @return A {@link DistributedLock} created for the given resource.
   */
  default DistributedLock getDistributedLock(String resource, String message) {
    return new DistributedLockLocalImpl();
  }

  /**
   * Enter maintenance mode.
   * @param reason The reason to enter maintenance mode.
   * @return True if cluster enters maintenance mode successfully.
   */
  default boolean enterMaintenanceMode(String reason) {
    return true;
  }

  /**
   * Exit maintenance mode.
   * @return True if cluster exits maintenance mode successfully.
   */
  default boolean exitMaintenanceMode() {
    return true;
  }

  /**
   * Update disk capacity in InstanceConfig.
   * @param diskCapacity The new disk capacity.
   * @return True if the update is a success.
   */
  default boolean updateDiskCapacity(int diskCapacity) {
    return true;
  }

  /**
   * Set disk state in DataNodeConfig.
   * @param diskId The disk to update state.
   * @param state The state to update to.
   * @return True if update is a success.
   */
  default boolean setDisksState(List<DiskId> diskId, HardwareState state) {
    return true;
  }

  /**
   * Given a map of old disks and new disks, swaps the old disks with the new disks in the DataNodeConfig,
   * and persists the resulting changes to Helix.
   * @param newDiskMapping A map of old disks to new disks.
   * @return {@code true} if the disks order was successfully updated, {@code false} otherwise.
   */
  default boolean setDisksOrder(Map<DiskId, DiskId> newDiskMapping) {
    // The default should be to do nothing about disk order, so return false.
    return false;
  }

  /**
   * Start the state machine model and registers the tasks
   * @param ambryStatsReports The list of {@link AmbryStatsReport} to start the state machine model.
   * @param accountStatsStore The {@link AccountStatsStore} to start the state machine model.
   * @param callback The callback to be called when the state machine model is started.
   */
  default void startStateMachineModel(List<AmbryStatsReport> ambryStatsReports, AccountStatsStore accountStatsStore,
      Callback<AggregatedAccountStorageStats> callback) {}
}
