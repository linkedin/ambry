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
package com.github.ambry.clustermap;

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

  public MockDataNodeId(ArrayList<Port> ports, List<String> mountPaths, String dataCenter) {
    this.mountPaths = mountPaths;
    this.datacenter = dataCenter;
    this.ports = new HashMap<PortType, Integer>();
    populatePorts(ports);
  }

  private void populatePorts(ArrayList<Port> ports) {
    boolean plainTextPortFound = false;
    for (Port port : ports) {
      if (port.getPortType() == PortType.PLAINTEXT) {
        plainTextPortFound = true;
        this.port = port.getPort();
      }
      this.ports.put(port.getPortType(), port.getPort());
    }
    if (!plainTextPortFound) {
      throw new IllegalArgumentException("No Plain Text port found");
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
    if (hasSSLPort()) {
      return ports.get(PortType.SSL);
    }
    throw new IllegalArgumentException("No SSL port exists for the datanode " + hostname + ":" + port);
  }

  @Override
  public boolean hasSSLPort() {
    if (ports.containsKey(PortType.SSL)) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public Port getPortToConnectTo(ArrayList<String> sslEnabledDataCenters) {
    if (sslEnabledDataCenters.contains(datacenter)) {
      if (ports.containsKey(PortType.SSL)) {
        return new Port(ports.get(PortType.SSL), PortType.SSL);
      } else {
        throw new IllegalArgumentException("No SSL Port exists for the data node " + hostname + ":" + port);
      }
    }
    return new Port(port, PortType.PLAINTEXT);
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
