package com.github.ambry.coordinator;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;

import java.util.HashMap;
import java.util.Map;


/**
 *
 */
public class MockCluster {
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
