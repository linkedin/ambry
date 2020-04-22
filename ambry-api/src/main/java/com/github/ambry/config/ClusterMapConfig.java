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
package com.github.ambry.config;

import org.apache.helix.model.LeaderStandbySMD;
import org.json.JSONObject;


/**
 * The configs for resource state.
 */
public class ClusterMapConfig {
  public static final String AMBRY_STATE_MODEL_DEF = "AmbryLeaderStandby";
  public static final String OLD_STATE_MODEL_DEF = LeaderStandbySMD.name;
  public static final String DEFAULT_STATE_MODEL_DEF = AMBRY_STATE_MODEL_DEF;
  private static final String MAX_REPLICAS_ALL_DATACENTERS = "max-replicas-all-datacenters";

  /**
   * The factory class used to get the resource state policies.
   */
  @Config("clustermap.resourcestatepolicy.factory")
  @Default("com.github.ambry.clustermap.FixedBackoffResourceStatePolicyFactory")
  public final String clusterMapResourceStatePolicyFactory;

  /**
   * The threshold for the number of consecutive errors to tolerate for a datanode.
   */
  @Config("clustermap.fixedtimeout.datanode.error.threshold")
  @Default("6")
  public final int clusterMapFixedTimeoutDatanodeErrorThreshold;

  /**
   * The time to wait before a datanode is retried after it has been determined to be down.
   */
  @Config("clustermap.fixedtimeout.datanode.retry.backoff.ms")
  @Default("5 * 60 * 1000")
  public final int clusterMapFixedTimeoutDataNodeRetryBackoffMs;

  /**
   * The threshold for the number of errors to tolerate for a disk.
   */
  @Config("clustermap.fixedtimeout.disk.error.threshold")
  @Default("1")
  public final int clusterMapFixedTimeoutDiskErrorThreshold;

  /**
   * The time to wait before a disk is retried after it has been determined to be down.
   */
  @Config("clustermap.fixedtimeout.disk.retry.backoff.ms")
  @Default("10 * 60 * 1000")
  public final int clusterMapFixedTimeoutDiskRetryBackoffMs;

  /**
   * The threshold for the number of errors to tolerate for a replica.
   */
  @Config("clustermap.fixedtimeout.replica.error.threshold")
  @Default("1")
  public final int clusterMapFixedTimeoutReplicaErrorThreshold;

  /**
   * The time to wait before a replica is retried after it has been determined to be down.
   */
  @Config("clustermap.fixedtimeout.replica.retry.backoff.ms")
  @Default("10 * 60 * 1000")
  public final int clusterMapFixedTimeoutReplicaRetryBackoffMs;

  /**
   * List of Datacenters to which local node needs SSL encryption to communicate
   */
  @Config("clustermap.ssl.enabled.datacenters")
  @Default("")
  public final String clusterMapSslEnabledDatacenters;

  /**
   * The clustermap agent factory to use for instantiating the Cluster Map and the Cluster Participant.
   */
  @Config("clustermap.clusteragents.factory")
  @Default("com.github.ambry.clustermap.StaticClusterAgentsFactory")
  public final String clusterMapClusterAgentsFactory;

  /**
   * The cluster change handler to use for Cluster Map.
   */
  @Config("clustermap.cluster.change.handler.type")
  @Default("SimpleClusterChangeHandler")
  public final String clusterMapClusterChangeHandlerType;

  /**
   * Serialized json containing the information about all the zk hosts that the Helix based cluster manager should
   * be aware of. This information should be of the following form:
   * <pre>
   * {
   *   "zkInfo" : [
   *     {
   *       "datacenter":"dc1",
   *       "id": "1",
   *       "zkConnectStr":"abc.example.com:2199,xyz.example.com:2299",
   *     },
   *     {
   *       "datacenter":"dc2",
   *       "id" : "2",
   *       "zkConnectStr":"def.example.com:2300",
   *     },
   *     {
   *       "datacenter":"cloud-dc",
   *       "id" : "3",
   *       "replicaType": "CLOUD_BACKED"
   *     }
   *   ]
   * }
   * </pre>
   * Also, there could be multiple zk connection strings within same datacenter(except for cloud-dc). The intention is to
   * allow ambry server to participate into multiple clusters for special case like migrating ambry to another ZK. For
   * ambry frontend, if there are multiple zk connection strings, only the first one will be adopted.
   */
  @Config("clustermap.dcs.zk.connect.strings")
  @Default("")
  public final String clusterMapDcsZkConnectStrings;

  /**
   * The name of the associated cluster for this node.
   */
  @Config("clustermap.cluster.name")
  public final String clusterMapClusterName;

  /**
   * The name of the associated datacenter for this node.
   */
  @Config("clustermap.datacenter.name")
  public final String clusterMapDatacenterName;

  /**
   * The host name associated with this node.
   */
  @Config("clustermap.host.name")
  public final String clusterMapHostName;

  /**
   * The port number associated with this node.
   */
  @Config("clustermap.port")
  @Default("null")
  public final Integer clusterMapPort;

  /**
   * Indicates if a reverse DNS lookup should be used to try and obtain the fully qualified domain names of cluster map
   * host entries. By default this is enabled and disabling should only be needed when a node's name cannot be
   * looked-up via a reverse lookup. For example when the node is known to Ambry by a CNAME record.
   *
   * Beware that disabling this option also prevents Ambry from checking if a nodes naming configuration is
   * correct. For example there is no way for Ambry to check if the node config for 'host1.example.com' is actually
   * deployed on a host called 'host1.example.com'.
   */
  @Config("clustermap.resolve.hostnames")
  @Default("true")
  public final boolean clusterMapResolveHostnames;

  /**
   * The partition class to assign to a partition if one is not supplied
   */
  @Config("clustermap.default.partition.class")
  @Default(MAX_REPLICAS_ALL_DATACENTERS)
  public final String clusterMapDefaultPartitionClass;

  /**
   * The current xid for this cluster manager. Any changes beyond this xid will be ignored by the cluster manager.
   */
  @Config("clustermap.current.xid")
  @Default("Long.MAX_VALUE")
  public final Long clustermapCurrentXid;

  /**
   * Indicate if cluster manager enables override on properties of partition. These properties include partition state
   * and partition class etc.
   * By default this config is disabled, the state of partition is dynamically updated based on SEALED list from Helix.
   * When something goes bad and partition override is enabled, cluster manager uses partition properties in Helix PropertyStore
   * as source of truth to resolve partition state and ignores any changes from SEALED list in InstanceConfig.
   */
  @Config("clustermap.enable.partition.override")
  @Default("false")
  public final boolean clusterMapEnablePartitionOverride;

  /**
   * If set to false, the Helix based cluster manager will only listen to changes to the cluster in the local colo. It
   * will only connect to the remote ZK servers during initialization.
   */
  @Config("clustermap.listen.cross.colo")
  @Default("true")
  public final boolean clustermapListenCrossColo;

  /**
   * Name of the datacenter of vcr nodes. It is expected that all the vcr nodes will reside in the same datacenter.
   */
  @Config("clustermap.vcr.datacenter.name")
  public final String clustermapVcrDatacenterName;

  /**
   * State model definition to register with helix cluster.
   */
  @Config("clustermap.state.model.definition")
  @Default(DEFAULT_STATE_MODEL_DEF)
  public final String clustermapStateModelDefinition;

  /**
   * Whether to enable state model listeners to take actions when state transition occurs.
   */
  @Config("clustermap.enable.state.model.listener")
  @Default("false")
  public final boolean clustermapEnableStateModelListener;

  /**
   * The maximum replication lag in bytes that is acceptable to consider replica has caught up.
   */
  @Config("clustermap.replica.catchup.acceptable.lag.bytes")
  public final long clustermapReplicaCatchupAcceptableLagBytes;

  /**
   * The minimum number of peers that a bootstrap replica is required to catch up with. If target is set to 0, then
   * {@link com.github.ambry.clustermap.ReplicaSyncUpManager} will use number of replicas in local dc as catchup target.
   */
  @Config("clustermap.replica.catchup.target")
  public final int clustermapReplicaCatchupTarget;

  /**
   * The minimum number of replicas in local datacenter required for a partition to serve PUT request. This is used to
   * get writable partitions for PUT operation. Any partition with replica count larger than or equal to this number is
   * acceptable to be considered as a candidate.
   */
  @Config("clustermap.writable.partition.min.replica.count")
  public final int clustermapWritablePartitionMinReplicaCount;

  /**
   * Whether to allow participant to dynamically update its datanode info in cluster.
   */
  @Config("clustermap.update.datanode.info")
  @Default("false")
  public final boolean clustermapUpdateDatanodeInfo;

  /**
   * Partition layout json for recovery test cluster map.
   */
  @Config("clustermap.recovery.test.partition.layout")
  @Default("{}")
  public final JSONObject clustermapRecoveryTestPartitionLayout;

  /**
   * Hardware layout json for recovery test cluster map.
   */
  @Config("clustermap.recovery.test.hardware.layout")
  @Default("{}")
  public final JSONObject clustermapRecoveryTestHardwareLayout;

  public ClusterMapConfig(VerifiableProperties verifiableProperties) {
    clusterMapFixedTimeoutDatanodeErrorThreshold =
        verifiableProperties.getIntInRange("clustermap.fixedtimeout.datanode.error.threshold", 3, 1, 100);
    clusterMapResourceStatePolicyFactory = verifiableProperties.getString("clustermap.resourcestatepolicy.factory",
        "com.github.ambry.clustermap.FixedBackoffResourceStatePolicyFactory");
    clusterMapFixedTimeoutDataNodeRetryBackoffMs =
        verifiableProperties.getIntInRange("clustermap.fixedtimeout.datanode.retry.backoff.ms", 5 * 60 * 1000, 1,
            20 * 60 * 1000);
    clusterMapFixedTimeoutDiskErrorThreshold =
        verifiableProperties.getIntInRange("clustermap.fixedtimeout.disk.error.threshold", 1, 1, 100);
    clusterMapFixedTimeoutDiskRetryBackoffMs =
        verifiableProperties.getIntInRange("clustermap.fixedtimeout.disk.retry.backoff.ms", 10 * 60 * 1000, 1,
            30 * 60 * 1000);
    clusterMapFixedTimeoutReplicaErrorThreshold =
        verifiableProperties.getIntInRange("clustermap.fixedtimeout.replica.error.threshold", 1, 1, Integer.MAX_VALUE);
    clusterMapFixedTimeoutReplicaRetryBackoffMs =
        verifiableProperties.getIntInRange("clustermap.fixedtimeout.replica.retry.backoff.ms", 10 * 60 * 1000, 1,
            30 * 60 * 1000);
    clusterMapSslEnabledDatacenters = verifiableProperties.getString("clustermap.ssl.enabled.datacenters", "");
    clusterMapClusterAgentsFactory = verifiableProperties.getString("clustermap.clusteragents.factory",
        "com.github.ambry.clustermap.StaticClusterAgentsFactory");
    clusterMapClusterChangeHandlerType =
        verifiableProperties.getString("clustermap.cluster.change.handler.type", "SimpleClusterChangeHandler");
    clusterMapDcsZkConnectStrings = verifiableProperties.getString("clustermap.dcs.zk.connect.strings", "");
    clusterMapClusterName = verifiableProperties.getString("clustermap.cluster.name");
    clusterMapDatacenterName = verifiableProperties.getString("clustermap.datacenter.name");
    clusterMapHostName = verifiableProperties.getString("clustermap.host.name");
    clusterMapPort = verifiableProperties.getInteger("clustermap.port", null);
    clusterMapResolveHostnames = verifiableProperties.getBoolean("clustermap.resolve.hostnames", true);
    clusterMapDefaultPartitionClass =
        verifiableProperties.getString("clustermap.default.partition.class", MAX_REPLICAS_ALL_DATACENTERS);
    clustermapCurrentXid = verifiableProperties.getLong("clustermap.current.xid", Long.MAX_VALUE);
    clusterMapEnablePartitionOverride = verifiableProperties.getBoolean("clustermap.enable.partition.override", false);
    clustermapListenCrossColo = verifiableProperties.getBoolean("clustermap.listen.cross.colo", true);
    clustermapStateModelDefinition =
        verifiableProperties.getString("clustermap.state.model.definition", DEFAULT_STATE_MODEL_DEF);
    clustermapVcrDatacenterName = verifiableProperties.getString("clustermap.vcr.datacenter.name", null);
    if (!clustermapStateModelDefinition.equals(DEFAULT_STATE_MODEL_DEF) && !clustermapStateModelDefinition.equals(
        OLD_STATE_MODEL_DEF)) {
      throw new IllegalArgumentException("Unsupported state model definition: " + clustermapStateModelDefinition);
    }
    clustermapEnableStateModelListener =
        verifiableProperties.getBoolean("clustermap.enable.state.model.listener", false);
    clustermapReplicaCatchupAcceptableLagBytes =
        verifiableProperties.getLongInRange("clustermap.replica.catchup.acceptable.lag.bytes", 0L, 0L, Long.MAX_VALUE);
    clustermapReplicaCatchupTarget =
        verifiableProperties.getIntInRange("clustermap.replica.catchup.target", 0, 0, Integer.MAX_VALUE);
    clustermapWritablePartitionMinReplicaCount =
        verifiableProperties.getIntInRange("clustermap.writable.partition.min.replica.count", 3, 0, Integer.MAX_VALUE);
    clustermapUpdateDatanodeInfo = verifiableProperties.getBoolean("clustermap.update.datanode.info", false);
    clustermapRecoveryTestHardwareLayout =
        new JSONObject(verifiableProperties.getString("clustermap.recovery.test.hardware.layout", "{}"));
    clustermapRecoveryTestPartitionLayout =
        new JSONObject(verifiableProperties.getString("clustermap.recovery.test.partition.layout", "{}"));
  }
}
