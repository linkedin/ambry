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

import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.utils.Utils;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * DataNode is uniquely identified by its hostname and port. A DataNode is in a {@link Datacenter}. A DataNode has zero
 * or more {@link Disk}s.
 */
public class DataNode extends DataNodeId {
  private static final int MinPort = 1025;
  private static final int MaxPort = 65535;
  private static final int MissingRackId = -1;

  private final Datacenter datacenter;
  private final String hostname;
  private final int portNum;
  private final Map<PortType, Port> ports;
  private final ArrayList<Disk> disks;
  private final long rawCapacityInBytes;
  private final ResourceStatePolicy dataNodeStatePolicy;
  private final long rackId;
  private final ArrayList<String> sslEnabledDataCenters;

  private final Logger logger = LoggerFactory.getLogger(getClass());

  public DataNode(Datacenter datacenter, JSONObject jsonObject, ClusterMapConfig clusterMapConfig)
      throws JSONException {
    if (logger.isTraceEnabled()) {
      logger.trace("DataNode " + jsonObject.toString());
    }
    this.datacenter = datacenter;
    this.sslEnabledDataCenters = Utils.splitString(clusterMapConfig.clusterMapSslEnabledDatacenters, ",");

    this.hostname = getFullyQualifiedDomainName(jsonObject.getString("hostname"));
    this.portNum = jsonObject.getInt("port");
    try {
      ResourceStatePolicyFactory resourceStatePolicyFactory =
          Utils.getObj(clusterMapConfig.clusterMapResourceStatePolicyFactory, this,
              HardwareState.valueOf(jsonObject.getString("hardwareState")), clusterMapConfig);
      this.dataNodeStatePolicy = resourceStatePolicyFactory.getResourceStatePolicy();
    } catch (Exception e) {
      logger.error("Error creating resource state policy when instantiating a datanode " + e);
      throw new IllegalStateException(
          "Error creating resource state policy when instantiating a datanode: " + hostname + ":" + portNum, e);
    }
    JSONArray diskJSONArray = jsonObject.getJSONArray("disks");
    this.disks = new ArrayList<Disk>(diskJSONArray.length());
    for (int i = 0; i < diskJSONArray.length(); ++i) {
      this.disks.add(new Disk(this, diskJSONArray.getJSONObject(i), clusterMapConfig));
    }
    this.rawCapacityInBytes = calculateRawCapacityInBytes();
    this.ports = new HashMap<PortType, Port>();
    this.ports.put(PortType.PLAINTEXT, new Port(portNum, PortType.PLAINTEXT));
    populatePorts(jsonObject);

    if (jsonObject.has("rackId")) {
      this.rackId = jsonObject.getLong("rackId");
      if (this.rackId < 0) {
        throw new IllegalStateException("Invalid rackId : " + this.rackId + " is less than 0");
      }
    } else {
      this.rackId = MissingRackId;
    }

    validate();
  }

  private void populatePorts(JSONObject jsonObject) throws JSONException {
    if (jsonObject.has("sslport")) {
      int sslPortNum = jsonObject.getInt("sslport");
      this.ports.put(PortType.SSL, new Port(sslPortNum, PortType.SSL));
    }
  }

  /**
   * Converts a hostname into a canonical hostname.
   *
   * @param unqualifiedHostname hostname to be fully qualified
   * @return canonical hostname that can be compared with DataNode.getHostname()
   */
  public static String getFullyQualifiedDomainName(String unqualifiedHostname) {
    if (unqualifiedHostname == null) {
      throw new IllegalStateException("Hostname cannot be null.");
    } else if (unqualifiedHostname.length() == 0) {
      throw new IllegalStateException("Hostname cannot be zero length.");
    }

    try {
      return InetAddress.getByName(unqualifiedHostname).getCanonicalHostName().toLowerCase();
    } catch (UnknownHostException e) {
      throw new IllegalStateException(
          "Host (" + unqualifiedHostname + ") is unknown so cannot determine fully qualified domain name.");
    }
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
  public boolean hasSSLPort() {
    return ports.containsKey(PortType.SSL);
  }

  /**
   * Gets the DataNode's SSL port number.
   *
   * @return Port number upon which to establish an SSL encrypted connection with the DataNodeId.
   * @throws IllegalStateException Thrown if no SSL port exists.
   */
  @Override
  public int getSSLPort() {
    if (hasSSLPort()) {
      return ports.get(PortType.SSL).getPort();
    } else {
      throw new IllegalStateException("No SSL port exists for the Data Node " + hostname + ":" + portNum);
    }
  }

  /**
   * Returns the {@link Port} of this node to connect to. A {@link Port} will be automatically selected based on if
   * there is a need of establishing an SSL connection.
   *
   * @return {@link Port} to which the caller can connect to.
   * @throws IllegalStateException Thrown if an SSL connection is needed but no SSL port exists.
   */
  @Override
  public Port getPortToConnectTo() {
    if (sslEnabledDataCenters.contains(datacenter.getName())) {
      if (ports.containsKey(PortType.SSL)) {
        return ports.get(PortType.SSL);
      } else {
        throw new IllegalStateException("No SSL Port exists for the data node " + hostname + ":" + portNum);
      }
    }
    return ports.get(PortType.PLAINTEXT);
  }

  @Override
  public HardwareState getState() {
    return dataNodeStatePolicy.isDown() ? HardwareState.UNAVAILABLE : HardwareState.AVAILABLE;
  }

  public void onNodeTimeout() {
    dataNodeStatePolicy.onError();
  }

  public void onNodeResponse() {
    dataNodeStatePolicy.onSuccess();
  }

  public boolean isDown() {
    return dataNodeStatePolicy.isDown();
  }

  @Override
  public String getDatacenterName() {
    return getDatacenter().getName();
  }

  public Datacenter getDatacenter() {
    return datacenter;
  }

  public long getRawCapacityInBytes() {
    return rawCapacityInBytes;
  }

  private long calculateRawCapacityInBytes() {
    long capacityInBytes = 0;
    for (Disk disk : disks) {
      capacityInBytes += disk.getRawCapacityInBytes();
    }
    return capacityInBytes;
  }

  public List<Disk> getDisks() {
    return disks;
  }

  @Override
  public long getRackId() {
    return rackId;
  }

  protected void validateDatacenter() {
    if (datacenter == null) {
      throw new IllegalStateException("Datacenter cannot be null.");
    }
  }

  private void validateHostname() {
    String fqdn = getFullyQualifiedDomainName(hostname);
    if (!fqdn.equals(hostname)) {
      throw new IllegalStateException(
          "Hostname for DataNode (" + hostname + ") does not match its fully qualified domain name: " + fqdn + ".");
    }
  }

  private void validatePorts() {
    Set<Integer> portNumbers = new HashSet<Integer>();
    for (PortType portType : ports.keySet()) {
      int portNo = ports.get(portType).getPort();
      if (portNumbers.contains(portNo)) {
        throw new IllegalStateException("Same port number " + portNo + " found for two port types");
      }
      if (portNo < MinPort) {
        throw new IllegalStateException("Invalid " + portType + " port : " + portNo + " is less than " + MinPort);
      } else if (portNo > MaxPort) {
        throw new IllegalStateException("Invalid " + portType + " port : " + portNo + " is greater than " + MaxPort);
      }
      portNumbers.add(portNo);
    }
  }

  private void validate() {
    logger.trace("begin validate.");
    validateDatacenter();
    validateHostname();
    validatePorts();
    for (Disk disk : disks) {
      disk.validate();
    }
    logger.trace("complete validate.");
  }

  public JSONObject toJSONObject() throws JSONException {
    JSONObject jsonObject = new JSONObject().put("hostname", hostname).put("port", portNum);
    addSSLPortToJson(jsonObject);
    if (rackId >= 0) {
      jsonObject.put("rackId", getRackId());
    }
    jsonObject.put("hardwareState",
        dataNodeStatePolicy.isHardDown() ? HardwareState.UNAVAILABLE : HardwareState.AVAILABLE)
        .put("disks", new JSONArray());
    for (Disk disk : disks) {
      jsonObject.accumulate("disks", disk.toJSONObject());
    }
    return jsonObject;
  }

  private void addSSLPortToJson(JSONObject jsonObject) throws JSONException {
    for (PortType portType : ports.keySet()) {
      if (portType == PortType.SSL) {
        jsonObject.put("sslport", ports.get(portType).getPort());
      }
    }
  }

  @Override
  public String toString() {
    return "DataNode[" + getHostname() + ":" + getPort() + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DataNode dataNode = (DataNode) o;

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

    DataNode other = (DataNode) o;
    int compare = (portNum < other.portNum) ? -1 : ((portNum == other.portNum) ? 0 : 1);
    if (compare == 0) {
      compare = hostname.compareTo(other.hostname);
    }
    return compare;
  }
}
