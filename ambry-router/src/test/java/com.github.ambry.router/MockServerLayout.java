/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
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
import java.util.Collection;
import java.util.HashMap;
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
  }

  /**
   * Get the {@link MockServer} associated with the given host and port. Creates one if there is not any.
   * @param host the host
   * @param port the port
   * @return the {@link MockServer} associated with this host and port.
   */
  public synchronized MockServer getMockServer(String host, int port) {
    DataNodeId dataNodeId = clustermap.getDataNodeId(host, port);
    if (!mockServers.containsKey(dataNodeId)) {
      mockServers.put(dataNodeId, new MockServer());
    }
    return mockServers.get(dataNodeId);
  }

  /**
   * Return all the {@link MockServer}s contained within this server layout.
   * @return the list of {@link MockServer}s.
   */
  public Collection<MockServer> getMockServers() {
    return mockServers.values();
  }
}

