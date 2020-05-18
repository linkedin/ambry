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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * A data object for configs scoped to a single data node.
 */
class DataNodeConfig {
  private final String instanceName;
  private final String hostName;
  private final int port;
  private final String datacenterName;
  private final Integer sslPort;
  private final Integer http2Port;
  private final String rackId;
  private final long xid;
  private final Set<String> sealedReplicas = new HashSet<>();
  private final Set<String> stoppedReplicas = new HashSet<>();
  private final Map<String, DiskConfig> diskConfigs = new HashMap<>();

  /**
   * @param instanceName a name that can be used as a unique key for this server.
   * @param hostName the host name of the server.
   * @param port the port of the server.
   * @param datacenterName the datacenter this server is in.
   * @param sslPort the ssl port, or {@code null} if the server does not have one.
   * @param http2Port the HTTP2 port, or {@code null} if the server does not have one.
   * @param rackId an identifier for the rack or cabinet that the server is in for computing failure domains.
   * @param xid  the xid for this server.
   */
  DataNodeConfig(String instanceName, String hostName, int port, String datacenterName, Integer sslPort,
      Integer http2Port, String rackId, long xid) {
    this.instanceName = instanceName;
    this.hostName = hostName;
    this.port = port;
    this.datacenterName = datacenterName;
    this.sslPort = sslPort;
    this.http2Port = http2Port;
    this.rackId = rackId;
    this.xid = xid;
  }

  /**
    * @return a name that can be used as a unique key for this server.
   */
  String getInstanceName() {
    return instanceName;
  }

  /**
   * @return the host name of the server.
   */
  String getHostName() {
    return hostName;
  }

  /**
   * @return the port of the server.
   */
  int getPort() {
    return port;
  }

  /**
   * @return the datacenter this server is in.
   */
  String getDatacenterName() {
    return datacenterName;
  }

  /**
   * @return the ssl port, or {@code null} if the server does not have one.
   */
  Integer getSslPort() {
    return sslPort;
  }

  /**
   * @return the HTTP2 port, or {@code null} if the server does not have one.
   */
  Integer getHttp2Port() {
    return http2Port;
  }

  /**
   * @return an identifier for the rack or cabinet that the server is in for computing failure domains.
   */
  String getRackId() {
    return rackId;
  }

  /**
   * @return the xid for this server. After {@link SimpleClusterChangeHandler} is retired, this field will be removed.
   */
  long getXid() {
    return xid;
  }

  /**
   * @return the set of sealed replicas on this server. This set is mutable.
   */
  Set<String> getSealedReplicas() {
    return sealedReplicas;
  }

  /**
   * @return the set of stopped replicas on this server. This set is mutable.
   */
  Set<String> getStoppedReplicas() {
    return stoppedReplicas;
  }

  /**
   * @return a map from mount path to {@link DiskConfig} for all the disks on the server. This map is mutable.
   */
  Map<String, DiskConfig> getDiskConfigs() {
    return diskConfigs;
  }

  @Override
  public String toString() {
    return "ServerConfig{" + "instanceName='" + instanceName + '\'' + ", hostName='" + hostName + '\'' + ", port="
        + port + ", datacenterName='" + datacenterName + '\'' + ", sslPort=" + sslPort + ", http2Port=" + http2Port
        + ", rackId='" + rackId + '\'' + ", xid=" + xid + ", sealedReplicas=" + sealedReplicas + ", stoppedReplicas="
        + stoppedReplicas + ", diskConfigs=" + diskConfigs + '}';
  }

  /**
   * Configuration scoped to a single disk on a server.
   */
  static class DiskConfig {
    private final HardwareState state;
    private final long diskCapacity;
    private final Map<String, ReplicaConfig> replicaConfigs = new HashMap<>();

    /**
     * @param state the configured {@link HardwareState} of the disk.
     * @param diskCapacity the capacity of the disk in bytes.
     */
    DiskConfig(HardwareState state, long diskCapacity) {
      this.state = state;
      this.diskCapacity = diskCapacity;
    }

    /**
     * @return the configured {@link HardwareState} of the disk.
     */
    HardwareState getState() {
      return state;
    }

    /**
     * @return the capacity of the disk in bytes.
     */
    long getDiskCapacity() {
      return diskCapacity;
    }

    /**
     * @return a map from partition name to {@link ReplicaConfig} for all the repliccas on the server.
     *         This map is mutable.
     */
    Map<String, ReplicaConfig> getReplicaConfigs() {
      return replicaConfigs;
    }

    @Override
    public String toString() {
      return "DiskConfig{" + "state=" + state + ", diskCapacity=" + diskCapacity + ", replicaConfigs=" + replicaConfigs
          + '}';
    }
  }

  /**
   * Configuration scoped to a single replica on a disk.
   */
  static class ReplicaConfig {
    private final long replicaCapacity;
    private final String partitionClass;

    /**
     * @param replicaCapacity the capacity of this replica in bytes.
     * @param partitionClass the partition class of this replica.
     */
    ReplicaConfig(long replicaCapacity, String partitionClass) {
      this.replicaCapacity = replicaCapacity;
      this.partitionClass = partitionClass;
    }

    /**
     * @return the capacity of this replica in bytes.
     */
    long getReplicaCapacity() {
      return replicaCapacity;
    }

    /**
     * @return the partition class of this replica.
     */
    String getPartitionClass() {
      return partitionClass;
    }

    @Override
    public String toString() {
      return "ReplicaConfig{" + "replicaCapacity=" + replicaCapacity + ", partitionClass='" + partitionClass + '\''
          + '}';
    }
  }
}
