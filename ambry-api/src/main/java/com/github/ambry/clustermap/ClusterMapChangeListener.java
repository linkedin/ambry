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
package com.github.ambry.clustermap;

import java.util.List;


/**
 * A {@link ClusterMap} listener that takes actions on local node when cluster map is changed.
 */
public interface ClusterMapChangeListener {
  /**
   * Take actions when replicas are added or removed on local node.
   * @param addedReplicas {@link ReplicaId}(s) that have been added.
   * @param removedReplicas {@link ReplicaId}(s) that have been removed.
   */
  void onReplicaAddedOrRemoved(List<ReplicaId> addedReplicas, List<ReplicaId> removedReplicas);

  /**
   * Take actions when a data node is removed from the clustermap
   * @param removedDataNode {@link DataNodeId} that has been removed
   */
  default void onDataNodeRemoved(DataNodeId removedDataNode) {
  }

  /**
   * Take actions when there is a routing table update. This is triggered whenever there is any change to state of a replicas in the cluster.
   * On this trigger, we can look up the latest states of all the replicas from the routing table snapshot {@link org.apache.helix.spectator.RoutingTableSnapshot}
   * with the help of various APIs provided in its class.
   */
  default void onRoutingTableChange() {
  }
}
