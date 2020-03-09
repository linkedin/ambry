/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
 *
 */

package com.github.ambry.clustermap;

import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.utils.Utils;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONObject;

import static com.github.ambry.clustermap.ClusterMapSnapshotConstants.*;
import static com.github.ambry.clustermap.ClusterMapUtils.*;


/**
 *
 * This implementation of {@link AmbryDataNode} represents a standard ambry-server host with physical disks.
 */
class AmbryServerDataNode extends AmbryDataNode {
  private final String rackId;
  private final long xid;
  private final List<String> sslEnabledDataCenters;
  private final ClusterManagerCallback<AmbryReplica, AmbryDisk, AmbryPartition, AmbryDataNode> clusterManagerCallback;

  /**
   * Instantiate an {@link AmbryServerDataNode}.
   * @param dataCenterName the name of the dataCenter associated with this data node.
   * @param clusterMapConfig the {@link ClusterMapConfig} to use.
   * @param hostName the hostName identifying this data node.
   * @param portNum the port identifying this data node.
   * @param rackId the rack Id associated with this data node (may be null).
   * @param sslPortNum the ssl port associated with this data node (may be null).
   * @param http2PortNumber the http2 ssl port associated with this data node (may be null).
   * @param xid the xid associated with this data node.
   * @param clusterManagerCallback the {@link ClusterManagerCallback} to use
   * @throws Exception if there is an exception in instantiating the {@link ResourceStatePolicy}
   */
  AmbryServerDataNode(String dataCenterName, ClusterMapConfig clusterMapConfig, String hostName, int portNum,
      String rackId, Integer sslPortNum, Integer http2PortNumber, long xid,
      ClusterManagerCallback<AmbryReplica, AmbryDisk, AmbryPartition, AmbryDataNode> clusterManagerCallback)
      throws Exception {
    super(dataCenterName, clusterMapConfig, hostName, portNum, sslPortNum, http2PortNumber);
    this.rackId = rackId;
    this.xid = xid;
    this.sslEnabledDataCenters = Utils.splitString(clusterMapConfig.clusterMapSslEnabledDatacenters, ",");
    this.clusterManagerCallback = clusterManagerCallback;
    validateHostName(clusterMapConfig.clusterMapResolveHostnames, hostName);
    validatePorts(plainTextPort, sslPort, http2Port, sslEnabledDataCenters.contains(dataCenterName));
  }

  @Override
  public Port getPortToConnectTo() {
    return sslEnabledDataCenters.contains(getDatacenterName()) ? sslPort : plainTextPort;
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
    if (hasHttp2Port()) {
      portsJson.put(PortType.HTTP2.name(), getHttp2Port());
    }
    portsJson.put(DATA_NODE_PORT_CONNECT_TO, getPortToConnectTo().getPort());
    snapshot.put(DATA_NODE_PORTS, portsJson);
    snapshot.put(DATA_NODE_RACK_ID, getRackId());
    snapshot.put(DATA_NODE_XID, getXid());
    snapshot.put(LIVENESS, getLiveness());
    JSONArray disksJson = new JSONArray();
    clusterManagerCallback.getDisks(this).forEach(disk -> disksJson.put(disk.getSnapshot()));
    snapshot.put(DATA_NODE_DISKS, disksJson);
    return snapshot;
  }

  @Override
  public String toString() {
    return "DataNode[" + getHostname() + ":" + getPort() + "]";
  }
}
