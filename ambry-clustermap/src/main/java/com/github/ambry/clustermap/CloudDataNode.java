/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.config.CloudConfig;
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
 * {@link DataNodeId} implementation to use within VCR cluster.
 */
public class CloudDataNode implements DataNodeId {
  private final String hostName;
  private final Port plainTextPort;
  private final Port sslPort;
  private final Port http2Port;
  private final String dataCenterName;
  private final boolean isSslEnabled;
  private final List<String> sslEnabledDataCenters;
  private final Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Instantiate a CloudDataNode object.
   * @param cloudConfig the {@link CloudConfig} to use.
   * @param clusterMapConfig the {@link ClusterMapConfig} to use.
   */
  public CloudDataNode(CloudConfig cloudConfig, ClusterMapConfig clusterMapConfig) {
    if (clusterMapConfig.clusterMapPort == null) {
      throw new IllegalArgumentException("PlainText port is not provided");
    }
    this.hostName = clusterMapConfig.clusterMapHostName;
    this.plainTextPort = new Port(clusterMapConfig.clusterMapPort, PortType.PLAINTEXT);
    this.sslPort = (cloudConfig.vcrSslPort != null) ? new Port(cloudConfig.vcrSslPort, PortType.SSL) : null;
    this.http2Port = (cloudConfig.vcrHttp2Port != null) ? new Port(cloudConfig.vcrHttp2Port, PortType.HTTP2) : null;
    this.dataCenterName = clusterMapConfig.clusterMapDatacenterName;
    this.sslEnabledDataCenters = Utils.splitString(clusterMapConfig.clusterMapSslEnabledDatacenters, ",");
    this.isSslEnabled = sslEnabledDataCenters.contains(dataCenterName);
    validateHostName(clusterMapConfig.clusterMapResolveHostnames, hostName);
    validatePorts(plainTextPort, sslPort, http2Port, isSslEnabled);
  }

  /**
   * Instantiate a CloudDataNode object from hostname, port and datacentername.
   */
  public CloudDataNode(String hostName, Port plainTextPort, Port sslPort, Port http2Port, String dataCenterName,
      ClusterMapConfig clusterMapConfig) {
    this.hostName = hostName;
    this.plainTextPort = plainTextPort;
    this.sslPort = sslPort;
    this.http2Port = http2Port;
    this.dataCenterName = dataCenterName;
    this.sslEnabledDataCenters = Utils.splitString(clusterMapConfig.clusterMapSslEnabledDatacenters, ",");
    this.isSslEnabled = sslEnabledDataCenters.contains(dataCenterName);
    validateHostName(clusterMapConfig.clusterMapResolveHostnames, hostName);
    validatePorts(plainTextPort, sslPort, http2Port, isSslEnabled);
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
  public boolean hasSSLPort() {
    return sslPort != null;
  }

  @Override
  public int getSSLPort() {
    if (hasSSLPort()) {
      return sslPort.getPort();
    } else {
      throw new IllegalStateException("No SSL port exists for the Data Node " + hostName + ":" + plainTextPort);
    }
  }

  @Override
  public boolean hasHttp2Port() {
    return http2Port != null;
  }

  @Override
  public int getHttp2Port() {
    if (hasHttp2Port()) {
      return http2Port.getPort();
    } else {
      throw new IllegalStateException("No HTTP2 port exists for the Data Node " + hostName + ":" + plainTextPort);
    }
  }

  @Override
  public Port getPortToConnectTo() {
    return isSslEnabled ? sslPort : plainTextPort;
  }

  @Override
  public HardwareState getState() {
    return HardwareState.AVAILABLE;
  }

  @Override
  public String getDatacenterName() {
    return dataCenterName;
  }

  @Override
  public String getRackId() {
    return null;
  }

  @Override
  public long getXid() {
    return 0;
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
    if (hasHttp2Port()) {
      portsJson.put(PortType.HTTP2.name(), getHttp2Port());
    }
    portsJson.put(DATA_NODE_PORT_CONNECT_TO, getPortToConnectTo().getPort());
    snapshot.put(DATA_NODE_PORTS, portsJson);
    snapshot.put(DATA_NODE_RACK_ID, getRackId());
    snapshot.put(DATA_NODE_XID, getXid());
    snapshot.put(LIVENESS, UP);
    return snapshot;
  }

  @Override
  public String toString() {
    return "CloudDataNode[" + getHostname() + ":" + getPort() + "]";
  }
}
