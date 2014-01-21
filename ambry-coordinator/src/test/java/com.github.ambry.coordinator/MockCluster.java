package com.github.ambry.coordinator;

import com.github.ambry.clustermap.DataNodeId;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
class MockCluster {
  private Map<DataNodeId, MockDataNode> mockDataNodes;

  public MockCluster() {
    this.mockDataNodes = new HashMap<DataNodeId, MockDataNode>();
  }

  public synchronized MockDataNode getMockDataNode(DataNodeId dataNodeId) {
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
