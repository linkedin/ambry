/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.protocol.RequestOrResponseType;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * A mock server layout (cluster) initialized with the given {@link ClusterMap}. Basically creates a unique
 * {@link MockServer} associated with every {@link DataNodeId} in the cluster map, but only on demand.
 */
class MockServerLayout {
  private Map<DataNodeId, MockServer> mockServers;
  private ClusterMap clustermap;

  /**
   * Construct a MockServerLayout.
   * @param clusterMap the {@link ClusterMap} used to associate a host and port with a MockServer.
   */
  public MockServerLayout(ClusterMap clusterMap) {
    this.mockServers = new HashMap<DataNodeId, MockServer>();
    this.clustermap = clusterMap;
    for (DataNodeId dataNodeId : clusterMap.getDataNodeIds()) {
      mockServers.put(dataNodeId, new MockServer(clusterMap, dataNodeId.getDatacenterName()));
    }
  }

  /**
   * Dynamically add new mock servers into layout.
   * @param newNodes a list of new nodes to add.
   * @param clusterMap the {@link ClusterMap} used to associate a host and port with a MockServer.
   */
  public void addMockServers(List<? extends DataNodeId> newNodes, ClusterMap clusterMap) {
    newNodes.forEach(node -> mockServers.putIfAbsent(node, new MockServer(clusterMap, node.getDatacenterName())));
  }

  /**
   * Get the {@link MockServer} associated with the given host and port. Creates one if there is not any.
   * @param host the host
   * @param port the port
   * @return the {@link MockServer} associated with this host and port.
   */
  public synchronized MockServer getMockServer(String host, int port) {
    DataNodeId dataNodeId = clustermap.getDataNodeId(host, port);
    return mockServers.get(dataNodeId);
  }

  /**
   * Return all the {@link MockServer}s contained within this server layout.
   * @return the list of {@link MockServer}s.
   */
  public Collection<MockServer> getMockServers() {
    return mockServers.values();
  }

  /**
   * Get the count of requests of the given type received within the servers in this layout.
   * @param type the type of request
   * @return the count of requests this server layout has received of the given type.
   */
  public int getCount(RequestOrResponseType type) {
    int count = 0;
    for (MockServer mockServer : mockServers.values()) {
      count += mockServer.getCount(type);
    }
    return count;
  }
}

