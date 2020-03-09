/*
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
import java.util.concurrent.atomic.AtomicBoolean;
import org.json.JSONArray;
import org.json.JSONObject;

import static com.github.ambry.clustermap.ClusterMapSnapshotConstants.*;


public class MockDataNodeId implements DataNodeId {
  private final Map<PortType, Port> ports;
  private final List<String> mountPaths;
  private final String hostname;
  private final String datacenter;
  private List<String> sslEnabledDataCenters = new ArrayList<String>();
  private int portNum;
  private AtomicBoolean isTimedout = new AtomicBoolean(false);

  /**
   * Create a {@link MockDataNodeId} object for disk based datanode.
   * @param hostname Hostname of the node.
   * @param ports Ports associated with server on node.
   * @param mountPaths Mount paths for replicas on node.
   * @param dataCenter Name of datacenter.
   */
  public MockDataNodeId(String hostname, List<Port> ports, List<String> mountPaths, String dataCenter) {
    this.hostname = hostname;
    this.mountPaths = mountPaths;
    this.datacenter = dataCenter;
    this.ports = new HashMap<>();
    populatePorts(ports);
  }

  /**
   * Create a {@link MockDataNodeId} object from given {@code ports}, {@code mountPaths}, {@code dataCenter}.
   * @param ports Ports associated with server on node.
   * @param mountPaths Mount paths for replicas on node.
   * @param dataCenter Name of datacenter.
   */
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
    return ports.containsKey(PortType.SSL);
  }

  @Override
  public int getHttp2Port() {
    if (hasHttp2Port()) {
      return ports.get(PortType.HTTP2).getPort();
    }
    throw new IllegalArgumentException("No HTTP2 port exists for the datanode " + hostname + ":" + portNum);
  }

  @Override
  public boolean hasHttp2Port() {
    return ports.containsKey(PortType.HTTP2);
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

  @Override
  public JSONObject getSnapshot() {
    JSONObject snapshot = new JSONObject();
    snapshot.put(DATA_NODE_HOSTNAME, getHostname());
    snapshot.put(DATA_NODE_DATACENTER, getDatacenterName());
    snapshot.put(DATA_NODE_SSL_ENABLED_DATACENTERS, new JSONArray(sslEnabledDataCenters));
    JSONObject portsJson = new JSONObject();
    ports.forEach((portType, port) -> portsJson.put(port.getPortType().name(), port.getPort()));
    portsJson.put(DATA_NODE_PORT_CONNECT_TO, getPortToConnectTo().getPort());
    snapshot.put(DATA_NODE_PORTS, portsJson);
    snapshot.put(DATA_NODE_XID, getXid());
    snapshot.put(LIVENESS, getState() == HardwareState.UNAVAILABLE ? DOWN : UP);
    JSONArray disksJson = new JSONArray();
    mountPaths.forEach(mountPath -> disksJson.put(new MockDiskId(this, mountPath).getSnapshot()));
    snapshot.put(DATA_NODE_DISKS, disksJson);
    return snapshot;
  }

  public List<String> getMountPaths() {
    return mountPaths;
  }

  /**
   * Add new mount paths to current datanode.
   * @param pathsToAdd a list of mount paths to be added onto current node
   */
  public void addMountPaths(List<String> pathsToAdd) {
    mountPaths.addAll(pathsToAdd);
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

  public void onNodeTimeout() {
    isTimedout.set(true);
  }

  public void onNodeResponse() {
    isTimedout.set(false);
  }

  public boolean isTimedOut() {
    return isTimedout.get();
  }

  @Override
  public String toString() {
    return datacenter + ":" + hostname + ":" + portNum;
  }
}
