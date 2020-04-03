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
import org.json.JSONObject;

import static com.github.ambry.clustermap.ClusterMapSnapshotConstants.*;
import static com.github.ambry.clustermap.ClusterMapUtils.*;


/**
 * This is a place for {@link CloudServiceReplica}s to live on. This represents a location for in-process managed
 * service clients (not a VCR like {@link CloudDataNode}). This will not contain any ports, as it is not meant to
 * represent a remote network location. The hostname will just be the datacenter name. For logging purposes, the
 * plaintext port will show as -1.
 */
class CloudServiceDataNode extends AmbryDataNode {

  /**
   * Instantiate a {@link CloudServiceDataNode}.
   * @param dataCenterName the name of the dataCenter associated with this data node.
   * @param clusterMapConfig the {@link ClusterMapConfig} to use.
   * @throws Exception if there is an exception in instantiating the {@link ResourceStatePolicy}
   */
  CloudServiceDataNode(String dataCenterName, ClusterMapConfig clusterMapConfig) throws Exception {
    super(dataCenterName, clusterMapConfig, dataCenterName, UNKNOWN_PORT, null, null);
  }

  /**
   * @return a port with number {@link #UNKNOWN_PORT}, since CloudServiceDataNode cannot be connected to via a socket.
   */
  @Override
  public Port getPortToConnectTo() {
    // plaintextPort is constructed using UNKNOWN_PORT
    return plainTextPort;
  }

  @Override
  public String getRackId() {
    return null;
  }

  @Override
  public long getXid() {
    return DEFAULT_XID;
  }

  @Override
  public JSONObject getSnapshot() {
    JSONObject snapshot = new JSONObject();
    snapshot.put(DATA_NODE_HOSTNAME, getHostname());
    snapshot.put(DATA_NODE_DATACENTER, getDatacenterName());
    snapshot.put(DATA_NODE_XID, getXid());
    snapshot.put(LIVENESS, getLiveness());
    return snapshot;
  }
}
