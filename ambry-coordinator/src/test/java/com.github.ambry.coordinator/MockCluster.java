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
package com.github.ambry.coordinator;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;

import java.util.HashMap;
import java.util.Map;


/**
 *
 */
class MockCluster {
  private Map<DataNodeId, MockDataNode> mockDataNodes;
  private ClusterMap clustermap;

  public MockCluster(ClusterMap clusterMap) {
    this.mockDataNodes = new HashMap<DataNodeId, MockDataNode>();
    this.clustermap = clusterMap;
  }

  public synchronized MockDataNode getMockDataNode(String host, int port) {
    DataNodeId dataNodeId = clustermap.getDataNodeId(host, port);
    if (!mockDataNodes.containsKey(dataNodeId)) {
      mockDataNodes.put(dataNodeId, new MockDataNode(dataNodeId));
    }
    return mockDataNodes.get(dataNodeId);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (MockDataNode mockDataNode : mockDataNodes.values()) {
      sb.append(mockDataNode);
    }
    return sb.toString();
  }
}
