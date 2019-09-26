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
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.clustermap.ClusterMapSnapshotConstants.*;
import static com.github.ambry.clustermap.ClusterMapUtils.*;


/**
 * {@link DataNodeId} implementation to use within dynamic cluster managers.
 */
class AmbryDataNode implements DataNodeId {
  private final String hostName;
  private final Port plainTextPort;
  private final Port sslPort;
  private final String dataCenterName;
  private final String rackId;
  private final long xid;
  private final List<String> sslEnabledDataCenters;
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final ResourceStatePolicy resourceStatePolicy;
  private final ClusterManagerCallback clusterManagerCallback;

  /**
   * Instantiate an AmbryDataNode object.
   * @param dataCenterName the name of the dataCenter associated with this data node.
   * @param clusterMapConfig the {@link ClusterMapConfig} to use.
   * @param hostName the hostName identifying this data node.
   * @param portNum the port identifying this data node.
   * @param rackId the rack Id associated with this data node (may be null).
   * @param sslPortNum the ssl port associated with this data node (may be null).
   * @param xid the xid associated with this data node.
   * @param clusterManagerCallback the {@link ClusterManagerCallback} to use
   * @throws Exception if there is an exception in instantiating the {@link ResourceStatePolicy}
   */
  AmbryDataNode(String dataCenterName, ClusterMapConfig clusterMapConfig, String hostName, int portNum, String rackId,
      Integer sslPortNum, long xid, ClusterManagerCallback clusterManagerCallback) throws Exception {
    this.hostName = hostName;
    this.plainTextPort = new Port(portNum, PortType.PLAINTEXT);
    this.sslPort = sslPortNum != null ? new Port(sslPortNum, PortType.SSL) : null;
    this.dataCenterName = dataCenterName;
    this.rackId = rackId;
    this.xid = xid;
    this.sslEnabledDataCenters = Utils.splitString(clusterMapConfig.clusterMapSslEnabledDatacenters, ",");
    ResourceStatePolicyFactory resourceStatePolicyFactory =
        Utils.getObj(clusterMapConfig.clusterMapResourceStatePolicyFactory, this, HardwareState.AVAILABLE,
            clusterMapConfig);
    this.resourceStatePolicy = resourceStatePolicyFactory.getResourceStatePolicy();
    this.clusterManagerCallback = clusterManagerCallback;
    validateHostName(clusterMapConfig.clusterMapResolveHostnames, hostName);
    validatePorts(plainTextPort, sslPort, sslEnabledDataCenters.contains(dataCenterName));
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
  public String getRackId() {
    return rackId;
  }

  @Override
  public long getXid() {
    return xid;
  }

  @Override
  public JSONObject getSnapshot() {
    JSONObject snapshot = new JSONObject();
    snapshot.put(DATA_NODE_HOSTNAME, getHostname());
    snapshot.put(DATA_NODE_DATACENTER, getDatacenterName());
    snapshot.put(DATA_NODE_SSL_ENABLED_DATACENTERS, new JSONArray(sslEnabledDataCenters));
    JSONObject portsJson = new JSONObject();
    portsJson.put(PortType.PLAINTEXT.name(), getPort());
    if (hasSSLPort()) {
      portsJson.put(PortType.SSL.name(), getSSLPort());
    }
    portsJson.put(DATA_NODE_PORT_CONNECT_TO, getPortToConnectTo().getPort());
    snapshot.put(DATA_NODE_PORTS, portsJson);
    snapshot.put(DATA_NODE_RACK_ID, getRackId());
    snapshot.put(DATA_NODE_XID, getXid());
    String liveness = UP;
    if (resourceStatePolicy.isHardDown()) {
      liveness = NODE_DOWN;
    } else if (resourceStatePolicy.isDown()) {
      liveness = SOFT_DOWN;
    }
    snapshot.put(LIVENESS, liveness);
    JSONArray disksJson = new JSONArray();
    clusterManagerCallback.getDisks(this).forEach(disk -> disksJson.put(disk.getSnapshot()));
    snapshot.put(DATA_NODE_DISKS, disksJson);
    return snapshot;
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
