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
package com.github.ambry.clustermap;

import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class MockDataNodeId implements DataNodeId {
  private final Map<PortType, Port> ports;
  private final List<String> mountPaths;
  private final String hostname;
  private final String datacenter;
  private List<String> sslEnabledDataCenters = new ArrayList<String>();
  private int portNum;

  public MockDataNodeId(String hostname, List<Port> ports, List<String> mountPaths, String dataCenter) {
    this.hostname = hostname;
    this.mountPaths = mountPaths;
    this.datacenter = dataCenter;
    this.ports = new HashMap<>();
    populatePorts(ports);
  }

  public MockDataNodeId(List<Port> ports, List<String> mountPaths, String dataCenter) {
    this("localhost", ports, mountPaths, dataCenter);
  }

  private void populatePorts(List<Port> ports) {
    boolean plainTextPortFound = false;
    for (Port port : ports) {
      if (port.getPortType() == PortType.PLAINTEXT) {
        plainTextPortFound = true;
        this.portNum = port.getPort();
      }
      this.ports.put(port.getPortType(), port);
    }
    if (!plainTextPortFound) {
      throw new IllegalArgumentException("No Plain Text port found");
    }
  }

  /**
   * Set the datacenters to which ssl is enabled. If the datacenter on which this datanode resides is
   * part of the datacenters to which ssl is enabled, then this datanode will always return an SSL port in
   * {@link #getPortToConnectTo()}
   * @param sslEnabledDataCenters list of datacenters to which ssl is enabled.
   */
  public void setSslEnabledDataCenters(List<String> sslEnabledDataCenters) {
    this.sslEnabledDataCenters = sslEnabledDataCenters;
  }

  @Override
  public String getHostname() {
    return hostname;
  }

  @Override
  public int getPort() {
    return portNum;
  }

  @Override
  public int getSSLPort() {
    if (hasSSLPort()) {
      return ports.get(PortType.SSL).getPort();
    }
    throw new IllegalArgumentException("No SSL port exists for the datanode " + hostname + ":" + portNum);
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
  public Port getPortToConnectTo() {
    if (sslEnabledDataCenters.contains(datacenter)) {
      if (ports.containsKey(PortType.SSL)) {
        return ports.get(PortType.SSL);
      } else {
        throw new IllegalArgumentException("No SSL Port exists for the data node " + hostname + ":" + portNum);
      }
    }
    return new Port(portNum, PortType.PLAINTEXT);
  }

  @Override
  public String getDatacenterName() {
    return datacenter;
  }

  @Override
  public HardwareState getState() {
    return HardwareState.AVAILABLE;
  }

  @Override
  public String getRackId() {
    return null;
  }

  @Override
  public long getXid() {
    return Long.MIN_VALUE;
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

    if (portNum != dataNode.portNum) {
      return false;
    }
    return hostname.equals(dataNode.hostname);
  }

  @Override
  public int hashCode() {
    int result = hostname.hashCode();
    result = 31 * result + portNum;
    return result;
  }

  @Override
  public int compareTo(DataNodeId o) {
    if (o == null) {
      throw new NullPointerException("input argument null");
    }

    MockDataNodeId other = (MockDataNodeId) o;
    int compare = (portNum < other.portNum) ? -1 : ((portNum == other.portNum) ? 0 : 1);
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

  @Override
  public String toString() {
    return datacenter + ":" + hostname + ":" + portNum;
  }
}
