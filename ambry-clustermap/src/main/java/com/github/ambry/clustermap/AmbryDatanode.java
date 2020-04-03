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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.clustermap.ClusterMapSnapshotConstants.*;
import static com.github.ambry.clustermap.ClusterMapUtils.*;


/**
 * {@link DataNodeId} implementation to use within dynamic cluster managers.
 */
abstract class AmbryDataNode implements DataNodeId {
  private final String hostName;
  // exposed for subclass access
  protected final Port plainTextPort;
  protected final Port sslPort;
  protected final Port http2Port;
  private final String dataCenterName;
  private final ResourceStatePolicy resourceStatePolicy;
  private static final Logger logger = LoggerFactory.getLogger(AmbryDataNode.class);

  /**
   * Instantiate an AmbryDataNode object.
   * @param dataCenterName the name of the dataCenter associated with this data node.
   * @param clusterMapConfig the {@link ClusterMapConfig} to use.
   * @param hostName the hostName identifying this data node.
   * @param plaintextPortNum the plaintext port identifying this data node. This can be
   *        {@link DataNodeId#UNKNOWN_PORT} if this object represents an in-process entity without a real TCP
   *        port.
   * @param sslPortNum the ssl port associated with this data node (may be null).
   * @param http2PortNum the http2 ssl port associated with this data node (may be null).
   * @throws Exception if there is an exception in instantiating the {@link ResourceStatePolicy}
   */
  AmbryDataNode(String dataCenterName, ClusterMapConfig clusterMapConfig, String hostName, int plaintextPortNum,
      Integer sslPortNum, Integer http2PortNum) throws Exception {
    this.hostName = hostName;
    this.dataCenterName = dataCenterName;
    this.plainTextPort = new Port(plaintextPortNum, PortType.PLAINTEXT);
    this.sslPort = sslPortNum != null ? new Port(sslPortNum, PortType.SSL) : null;
    this.http2Port = http2PortNum != null ? new Port(http2PortNum, PortType.HTTP2) : null;
    ResourceStatePolicyFactory resourceStatePolicyFactory =
        Utils.getObj(clusterMapConfig.clusterMapResourceStatePolicyFactory, this, HardwareState.AVAILABLE,
            clusterMapConfig);
    this.resourceStatePolicy = resourceStatePolicyFactory.getResourceStatePolicy();
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
    if (hasSSLPort()) {
      return sslPort.getPort();
    } else {
      throw new IllegalStateException(
          "No HTTP2 port exists for the Data Node " + hostName + ":" + plainTextPort.getPort());
    }
  }

  @Override
  public boolean hasSSLPort() {
    return sslPort != null;
  }

  @Override
  public int getHttp2Port() {
    if (hasHttp2Port()) {
      return http2Port.getPort();
    } else {
      throw new IllegalStateException(
          "No HTTP2 port exists for the Data Node " + hostName + ":" + plainTextPort.getPort());
    }
  }

  @Override
  public boolean hasHttp2Port() {
    return http2Port != null;
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
  public String toString() {
    return "DataNode[" + getHostname() + ":" + getPort() + "]";
  }

  /**
   * Set the hard state of this data node dynamically.
   * @param newState the updated {@link HardwareState}
   */
  void setState(HardwareState newState) {
    logger.trace("Setting state of instance " + getInstanceName(hostName, getPort()) + " to " + newState);
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

  /**
   * @return the status of the node according to its {@link ResourceStatePolicy}
   */
  String getLiveness() {
    return resourceStatePolicy.isHardDown() ? NODE_DOWN : resourceStatePolicy.isDown() ? SOFT_DOWN : UP;
  }
}
