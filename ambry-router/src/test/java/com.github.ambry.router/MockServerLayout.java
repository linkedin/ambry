package com.github.ambry.router;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.utils.Time;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


class MockServerLayout {
  private final Map<DataNodeId, MockServer> mockServers;
  private final ClusterMap clustermap;
  private final Time time;

  public MockServerLayout(ClusterMap clusterMap, Time time, boolean serverNoResponse) {
    this.mockServers = new HashMap<DataNodeId, MockServer>();
    this.clustermap = clusterMap;
    this.time = time;
    for(DataNodeId dataNodeId : clusterMap.getDataNodeIds()) {
      mockServers.put(dataNodeId, new MockServer(time, serverNoResponse));
    }
  }

  public synchronized MockServer getMockServer(String host, int port) {
    DataNodeId dataNodeId = clustermap.getDataNodeId(host, port);
    if (!mockServers.containsKey(dataNodeId)) {
      throw new IllegalStateException("Every datanode should already have a mock server.");
    }
    return mockServers.get(dataNodeId);
  }

  public synchronized void putMockServer(DataNodeId dataNodeId, MockServer mockserver) {
    mockServers.put(dataNodeId, mockserver);
  }

  public Collection<MockServer> getServers() {
    return mockServers.values();
  }
}

