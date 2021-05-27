/*
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

import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.helix.api.listeners.IdealStateChangeListener;
import org.apache.helix.api.listeners.LiveInstanceChangeListener;
import org.apache.helix.api.listeners.RoutingTableChangeListener;
import org.apache.helix.spectator.RoutingTableSnapshot;


/**
 * An extension of {@link ClusterChangeHandler} for data centers that use helix for cluster management. This interface
 * implements various helix listeners and provides facilities for using routing tables.
 */
interface HelixClusterChangeHandler
    extends ClusterChangeHandler, DataNodeConfigChangeListener, LiveInstanceChangeListener, IdealStateChangeListener,
            RoutingTableChangeListener {

  /**
   * Set the initial snapshot in this {@link HelixClusterChangeHandler}.
   * @param routingTableSnapshot the snapshot to set
   */
  void setRoutingTableSnapshot(RoutingTableSnapshot routingTableSnapshot);

  /**
   * @return current snapshot held by this {@link HelixClusterChangeHandler}.
   */
  RoutingTableSnapshot getRoutingTableSnapshot();

  /**
   * Wait for initial notification during startup.
   * @throws InterruptedException
   */
  void waitForInitNotification() throws InterruptedException;

  @Override
  default Stream<AmbryReplica> getReplicaIdsByState(AmbryPartition partition, ReplicaState state) {
    String resourceName = getPartitionToResourceMap().get(partition.toPathString());
    return getRoutingTableSnapshot().getInstancesForResource(resourceName, partition.toPathString(), state.name())
        .stream()
        .map(instanceConfig -> getDataNode(instanceConfig.getInstanceName()))
        .map(dataNode -> getReplicaId(dataNode, partition.toPathString())).filter(Objects::nonNull);
  }

  @Override
  default Map<ReplicaState, List<AmbryReplica>> getSnapshotOfReplicaStates(AmbryPartition partition) {
    String resourceName = getPartitionToResourceMap().get(partition.toPathString());
    RoutingTableSnapshot snapshot = getRoutingTableSnapshot();
    Map<ReplicaState, List<AmbryReplica>> replicasByState = new HashMap<>();
    for (ReplicaState state : EnumSet.of(ReplicaState.OFFLINE, ReplicaState.BOOTSTRAP, ReplicaState.STANDBY,
        ReplicaState.LEADER, ReplicaState.INACTIVE)) {
      List<AmbryReplica> list = snapshot.getInstancesForResource(resourceName, partition.toPathString(), state.name())
          .stream()
          .map(instanceConfig -> getDataNode(instanceConfig.getInstanceName()))
          .map(dataNode -> getReplicaId(dataNode, partition.toPathString()))
          .filter(Objects::nonNull)
          .collect(Collectors.toList());
      replicasByState.put(state, list);
    }
    return replicasByState;
  }
}
