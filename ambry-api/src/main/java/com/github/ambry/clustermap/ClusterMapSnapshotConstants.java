/*
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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

/**
 * List of constants that an implementation of {@link ClusterMap#getSnapshot()} may use
 */
public class ClusterMapSnapshotConstants {

  // general
  // keys
  public static final String TIMESTAMP_MS = "timeStampMs";
  public static final String IMPLEMENTATION = "implementation";
  public static final String CLUSTER_NAME = "clusterName";
  public static final String DATACENTERS = "datacenters";
  public static final String PARTITIONS = "partitions";
  public static final String CAPACITY_BYTES = "capacityBytes";
  public static final String LIVENESS = "liveness";
  // values
  public static final String UP = "up";
  public static final String DOWN = "down";
  public static final String NODE_DOWN = "nodeDown";
  public static final String DISK_DOWN = "diskDown";
  public static final String SOFT_DOWN = "softDown";

  // datacenters
  public static final String DATACENTER_NAME = "name";
  public static final String DATACENTER_ID = "id";
  public static final String DATACENTER_NODES = "datanodes";

  // datanodes
  public static final String DATA_NODE_HOSTNAME = "hostname";
  public static final String DATA_NODE_PORTS = "ports";
  public static final String DATA_NODE_PORT_CONNECT_TO = "portToConnectTo";
  public static final String DATA_NODE_DATACENTER = "dc";
  public static final String DATA_NODE_SSL_ENABLED_DATACENTERS = "sslEnabledDcs";
  public static final String DATA_NODE_RACK_ID = "rackId";
  public static final String DATA_NODE_XID = "xid";
  public static final String DATA_NODE_DISKS = "disks";

  // disks
  public static final String DISK_NODE = "node";
  public static final String DISK_MOUNT_PATH = "mountPath";

  // partitions
  public static final String PARTITION_ID = "id";
  public static final String PARTITION_CLASS = "class";
  public static final String PARTITION_WRITE_STATE = "writeState";
  public static final String PARTITION_REPLICAS = "replicas";

  // replicas
  // keys
  public static final String REPLICA_WRITE_STATE = "state";
  public static final String REPLICA_PATH = "path";
  public static final String REPLICA_NODE = "node";
  public static final String REPLICA_PARTITION = "partition";
  public static final String REPLICA_DISK = "disk";
  public static final String REPLICA_TYPE = "type";
  // values
  public static final String REPLICA_STOPPED = "stopped";

  //the virtual mount point for the cloud replica in the cluster map
  public static final String CLOUD_REPLICA_MOUNT = "/vcr";
}
