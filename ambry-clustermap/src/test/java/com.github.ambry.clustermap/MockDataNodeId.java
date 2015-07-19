package com.github.ambry.clustermap;

import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.HardwareState;

import java.util.List;


public class MockDataNodeId extends DataNodeId {
  int port;
  int sslPort;
  List<String> mountPaths;
  String hostname = "localhost";
  String datacenter;

  public MockDataNodeId(int port, int sslPort, List<String> mountPaths, String dataCenter) {
    this.port = port;
    this.sslPort = sslPort;
    this.mountPaths = mountPaths;
    this.datacenter = dataCenter;
  }

  public MockDataNodeId(int port, List<String> mountPaths, String dataCenter) {
    this.port = port;
    this.sslPort = -1;
    this.mountPaths = mountPaths;
    this.datacenter = dataCenter;
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
  public int getSSLPort() {
    return sslPort;
  }

  @Override
  public String getDatacenterName() {
    return datacenter;
  }

  @Override
  public HardwareState getState() {
    return HardwareState.AVAILABLE;
  }

  public List<String> getMountPaths() {
    return mountPaths;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MockDataNodeId dataNode = (MockDataNodeId) o;

    if (port != dataNode.port) {
      return false;
    }
    return hostname.equals(dataNode.hostname);
  }

  @Override
  public int hashCode() {
    int result = hostname.hashCode();
    result = 31 * result + port;
    return result;
  }

  @Override
  public int compareTo(DataNodeId o) {
    if (o == null) {
      throw new NullPointerException("input argument null");
    }

    MockDataNodeId other = (MockDataNodeId) o;
    int compare = (port < other.port) ? -1 : ((port == other.port) ? 0 : 1);
    if (compare == 0) {
      compare = hostname.compareTo(other.hostname);
    }
    return compare;
  }

  public void onNodeTimeout() {
    /* no-op for now */
  }

  public void onNodeResponse() {
    /* no-op for now */
  }
}
