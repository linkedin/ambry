package com.github.ambry.clustermap;

import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.HardwareState;

import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class MockDataNodeId extends DataNodeId {
  int port;
  Map<PortType, Integer> ports;
  List<String> mountPaths;
  String hostname = "localhost";
  String datacenter;

  public MockDataNodeId(int port, ArrayList<Port> ports, List<String> mountPaths, String dataCenter) {
    this.port = port;
    this.mountPaths = mountPaths;
    this.datacenter = dataCenter;
    parsePorts(ports);
  }

  public MockDataNodeId(int port, List<String> mountPaths, String dataCenter) {
    this.port = port;
    this.mountPaths = mountPaths;
    this.datacenter = dataCenter;
    parsePorts(new ArrayList<Port>());
  }

  private void parsePorts(ArrayList<Port> ports) {
    this.ports = new HashMap<PortType, Integer>();
    this.ports.put(PortType.PLAINTEXT, port);
    for (Port extraPort : ports) {
      if (extraPort.getPortType() != PortType.PLAINTEXT) {
        this.ports.put(extraPort.getPortType(), extraPort.getPortNo());
      }
    }
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
    for (PortType portType : ports.keySet()) {
      if (portType == PortType.SSL) {
        return ports.get(portType);
      }
    }
    throw new IllegalArgumentException("No SSL port exists for the datanode " + hostname + ":" + port);
  }

  @Override
  public boolean isSSLPortExists() {
    for (PortType portType : ports.keySet()) {
      if (portType == PortType.SSL) {
        return true;
      }
    }
    return false;
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
