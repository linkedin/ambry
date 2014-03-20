package com.github.ambry.clustermap;

import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.HardwareState;

public class MockDataNodeId implements DataNodeId {
  int port;

  public MockDataNodeId(int port) {
    this.port = port;
  }

  @Override
  public String getHostname() {
    return "127.0.0.1";
  }

  @Override
  public int getPort() {
    return port;
  }

  @Override
  public String getDatacenterName() {
    return "Datacenter";
  }

  @Override
  public HardwareState getState() {
    return HardwareState.AVAILABLE;
  }
}
