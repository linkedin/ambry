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
import java.util.concurrent.ConcurrentHashMap;
import org.apache.helix.api.listeners.IdealStateChangeListener;
import org.apache.helix.api.listeners.InstanceConfigChangeListener;
import org.apache.helix.api.listeners.LiveInstanceChangeListener;
import org.apache.helix.api.listeners.RoutingTableChangeListener;
import org.apache.helix.spectator.RoutingTableSnapshot;


interface ClusterChangeHandler
    extends InstanceConfigChangeListener, LiveInstanceChangeListener, IdealStateChangeListener,
            RoutingTableChangeListener {

  void setRoutingTableSnapshot(RoutingTableSnapshot routingTableSnapshot);

  RoutingTableSnapshot getRoutingTableSnapshot();

  Map<AmbryDataNode, Set<AmbryDisk>> getDataNodeToDisksMap();

  AmbryDataNode getDataNode(String instanceName);

  AmbryReplica getReplicaId(AmbryDataNode ambryDataNode, String partitionName);

  List<AmbryReplica> getReplicaIds(AmbryDataNode ambryDataNode);

  List<AmbryDataNode> getAllDataNodes();

  Set<AmbryDisk> getDisks(AmbryDataNode ambryDataNode);

  Map<String, String> getPartitionToResourceMap();

  long getErrorCount();
}
