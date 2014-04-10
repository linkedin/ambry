package com.github.ambry.clustermap;

import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.HardwareState;

import java.util.List;

public class MockDataNodeId implements DataNodeId {
  int port;
  List<String> mountPaths;
  String hostname = "localhost";

  public MockDataNodeId(int port, List<String> mountPaths) {
    this.port = port;
    this.mountPaths = mountPaths;
  }

  @Override
  public String getHostname() {
    return hostname;
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

  public List<String> getMountPaths() {
    return mountPaths;
  }
}
