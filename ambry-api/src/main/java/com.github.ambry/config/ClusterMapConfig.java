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

/**
 * The configs for resource state.
 */
public class ClusterMapConfig {

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
   * Serialized json containing the information about all the zk hosts that the Helix based cluster manager should
   * be aware of. This information should be of the following form:
   *
   * {
   *   "zkInfo" : [
   *     {
   *       "datacenter":"dc1",
   *       "id": "1",
   *       "zkConnectStr":"abc.example.com:2199",
   *     },
   *     {
   *       "datacenter":"dc2",
   *       "id" : "2",
   *       "zkConnectStr":"def.example.com:2300",
   *     }
   *   ]
   * }
   *
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
    clusterMapSslEnabledDatacenters = verifiableProperties.getString("clustermap.ssl.enabled.datacenters", "");
    clusterMapClusterAgentsFactory = verifiableProperties.getString("clustermap.clusteragents.factory",
        "com.github.ambry.clustermap.StaticClusterAgentsFactory");
    clusterMapDcsZkConnectStrings = verifiableProperties.getString("clustermap.dcs.zk.connect.strings", "");
    clusterMapClusterName = verifiableProperties.getString("clustermap.cluster.name");
    clusterMapDatacenterName = verifiableProperties.getString("clustermap.datacenter.name");
    clusterMapHostName = verifiableProperties.getString("clustermap.host.name");
    clusterMapPort = verifiableProperties.getInteger("clustermap.port", null);
    clusterMapResolveHostnames = verifiableProperties.getBoolean("clustermap.resolve.hostnames", true);
  }
}
