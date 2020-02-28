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

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.api.listeners.IdealStateChangeListener;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.api.listeners.LiveInstanceChangeListener;
import org.apache.helix.api.listeners.RoutingTableChangeListener;
import org.apache.helix.spectator.RoutingTableSnapshot;


/**
 * General handler that handles any resource or state changes in cluster. It exposes API(s) for cluster manager to
 * access up-to-date cluster info. Each data center has its own {@link ClusterChangeHandler}.
 */
interface ClusterChangeHandler
    extends InstanceConfigChangeListener, LiveInstanceChangeListener, IdealStateChangeListener,
            RoutingTableChangeListener {
  /**
   * Register a listener of cluster map for any changes.
   * @param clusterMapChangeListener the {@link ClusterMapChangeListener} to add.
   */
  void registerClusterMapListener(ClusterMapChangeListener clusterMapChangeListener);

  /**
   * Set the initial snapshot in this {@link ClusterChangeHandler}.
   * @param routingTableSnapshot the snapshot to set
   */
  void setRoutingTableSnapshot(RoutingTableSnapshot routingTableSnapshot);

  /**
   * @return current snapshot held by this {@link ClusterChangeHandler}.
   */
  RoutingTableSnapshot getRoutingTableSnapshot();

  /**
   * @return a map from ambry data node to its disks.
   */
  Map<AmbryDataNode, Set<AmbryDisk>> getDataNodeToDisksMap();

  /**
   * Get ambry data node associated with given instance name.
   * @param instanceName associated with ambry node.
   * @return requested {@link AmbryDataNode}
   */
  AmbryDataNode getDataNode(String instanceName);

  /**
   * Get {@link AmbryReplica} on given node that belongs to specified partition.
   * @param ambryDataNode the node on which the replica resides.
   * @param partitionName name of partition which the replica belongs to.
   * @return requested {@link AmbryReplica}
   */
  AmbryReplica getReplicaId(AmbryDataNode ambryDataNode, String partitionName);

  /**
   * Get all replicas on given node.
   * @param ambryDataNode the node on which replicas reside
   * @return a list of {@link AmbryReplica} on given node.
   */
  List<AmbryReplica> getReplicaIds(AmbryDataNode ambryDataNode);

  /**
   * @return all {@link AmbryDataNode} tracked by this {@link ClusterChangeHandler}
   */
  List<AmbryDataNode> getAllDataNodes();

  /**
   * Get all disks belong to given data node.
   * @param ambryDataNode the node which the disks belong to.
   * @return a set of {@link AmbryDisk} that belongs to given node.
   */
  Set<AmbryDisk> getDisks(AmbryDataNode ambryDataNode);

  /**
   * @return a map from partition name to its corresponding resource name in this {@link ClusterChangeHandler}.
   */
  Map<String, String> getPartitionToResourceMap();

  /**
   * @return number of errors occurred during handling cluster changes.
   */
  long getErrorCount();

  /**
   * Wait for initial notification during startup.
   * @throws InterruptedException
   */
  void waitForInitNotification() throws InterruptedException;
}
