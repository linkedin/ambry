/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
import java.util.List;
import java.util.TreeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.clustermap.ClusterMapUtils.*;


/**
 * {@link DataNodeId} implementation to use within dynamic cluster managers.
 */
class AmbryDataNode extends DataNodeId implements Resource {
  private final String hostName;
  private final Port plainTextPort;
  private final Port sslPort;
  private final String dataCenterName;
  private final long rackId;
  private final List<String> sslEnabledDataCenters;
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final ResourceStatePolicy resourceStatePolicy;
  private final ClusterMapConfig clusterMapConfig;

  /**
   * Instantiate an AmbryDataNode object.
   * @param dataCenterName the name of the dataCenter associated with this data node.
   * @param clusterMapConfig the {@link ClusterMapConfig} to use.
   * @param hostName the hostName identifying this data node.
   * @param portNum the port identifying this data node.
   * @param rackId the rack Id associated with this data node (may be null).
   * @param sslPortNum the ssl port associated with this data node (may be null).
   * @throws Exception if there is an exception in instantiating the {@link ResourceStatePolicy}
   */
  AmbryDataNode(String dataCenterName, ClusterMapConfig clusterMapConfig, String hostName, int portNum, Long rackId,
      Integer sslPortNum) throws Exception {
    this.hostName = hostName;
    this.plainTextPort = new Port(portNum, PortType.PLAINTEXT);
    this.sslPort = sslPortNum != null ? new Port(sslPortNum, PortType.SSL) : null;
    this.dataCenterName = dataCenterName;
    this.clusterMapConfig = clusterMapConfig;
    this.rackId = rackId != null ? rackId : UNKNOWN_RACK_ID;
    this.sslEnabledDataCenters = Utils.splitString(clusterMapConfig.clusterMapSslEnabledDatacenters, ",");
    ResourceStatePolicyFactory resourceStatePolicyFactory =
        Utils.getObj(clusterMapConfig.clusterMapResourceStatePolicyFactory, this, HardwareState.AVAILABLE,
            clusterMapConfig);
    this.resourceStatePolicy = resourceStatePolicyFactory.getResourceStatePolicy();
    validate();
  }

  /**
   * Validate the constructed AmbryDataNode.
   */
  private void validate() {
    // validate hostname
    if (clusterMapConfig.clusterMapResolveHostnames) {
      String fqdn = getFullyQualifiedDomainName(hostName);
      if (!fqdn.equals(hostName)) {
        throw new IllegalStateException(
            "Hostname for AmbryDataNode (" + hostName + ") does not match its fully qualified domain name: " + fqdn);
      }
    }

    // validate ports
    TreeSet<Integer> ports = new TreeSet<>();
    ports.add(plainTextPort.getPort());
    if (sslPort != null) {
      if (sslPort.getPort() == plainTextPort.getPort()) {
        throw new IllegalStateException("Same port number for both plain and ssl ports");
      }
      ports.add(sslPort.getPort());
    } else if (sslEnabledDataCenters.contains(dataCenterName)) {
      throw new IllegalArgumentException("No SSL port to a datanode to which SSL is enabled.");
    }
    if (ports.first() < MIN_PORT || ports.last() > MAX_PORT) {
      throw new IllegalStateException("Ports " + ports + " not in valid range [" + MIN_PORT + " - " + MAX_PORT + "]");
    }
  }

  @Override
  public String getHostname() {
    return hostName;
  }

  @Override
  public int getPort() {
    return plainTextPort.getPort();
  }

  @Override
  public int getSSLPort() {
    return sslPort.getPort();
  }

  @Override
  public boolean hasSSLPort() {
    return sslPort != null;
  }

  @Override
  public Port getPortToConnectTo() {
    return sslEnabledDataCenters.contains(dataCenterName) ? sslPort : plainTextPort;
  }

  @Override
  public HardwareState getState() {
    return resourceStatePolicy.isDown() ? HardwareState.UNAVAILABLE : HardwareState.AVAILABLE;
  }

  @Override
  public String getDatacenterName() {
    return dataCenterName;
  }

  @Override
  public long getRackId() {
    return rackId;
  }

  @Override
  public int compareTo(DataNodeId o) {
    if (getClass() != o.getClass()) {
      throw new IllegalStateException("Incompatible objects to compare");
    }
    AmbryDataNode other = (AmbryDataNode) o;
    int compare = (plainTextPort.getPort() < other.plainTextPort.getPort()) ? -1
        : ((plainTextPort.getPort() == other.plainTextPort.getPort()) ? 0 : 1);
    if (compare == 0) {
      compare = hostName.compareTo(other.hostName);
    }
    return compare;
  }

  @Override
  public String toString() {
    return "DataNode[" + getHostname() + ":" + getPort() + "]";
  }

  /**
   * Set the hard state of this data node dynamically.
   * @param newState the updated {@link HardwareState}
   */
  void setState(HardwareState newState) {
    logger.trace("Setting state of instance " + getInstanceName(hostName, plainTextPort.getPort()) + " to " + newState);
    if (newState == HardwareState.AVAILABLE) {
      resourceStatePolicy.onHardUp();
    } else {
      resourceStatePolicy.onHardDown();
    }
  }

  /**
   * Take actions, if any, when a request to this node times out.
   */
  void onNodeTimeout() {
    resourceStatePolicy.onError();
  }

  /**
   * Take actions, if any, when a request to this node receives a response.
   */
  void onNodeResponse() {
    resourceStatePolicy.onSuccess();
  }
}

