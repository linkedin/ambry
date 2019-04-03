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
package com.github.ambry.config;

/**
 * The configs for cloud related configurations.
 */
public class CloudConfig {

  public static final String VIRTUAL_REPLICATOR_CLUSTER_FACTORY_CLASS = "virtual.replicator.cluster.factory.class";
  public static final String CLOUD_DESTINATION_FACTORY_CLASS = "cloud.destination.factory.class";
  public static final String VCR_CLUSTER_ZK_CONNECT_STRING = "vcr.cluster.zk.connect.string";
  public static final String VCR_CLUSTER_NAME = "vcr.cluster.name";
  public static final String VCR_SSL_PORT = "vcr.ssl.port";
  public static final String VCR_REPLICA_MOUNT_PATH_PREFIX = "vcr.replica.mount.path.prefix";
  public static final String VCR_REQUIRE_ENCRYPTION = "vcr.require.encryption";
  public static final String VCR_MIN_TTL_DAYS = "vcr.min.ttl.days";
  public static final String VCR_ASSIGNED_PARTITIONS = "vcr.assigned.partitions";

  private static final String DEFAULT_VIRTUAL_REPLICATOR_CLUSTER_FACTORY_CLASS =
      "com.github.ambry.cloud.StaticVcrClusterFactory";
  private static final String DEFAULT_CLOUD_DESTINATION_FACTORY_CLASS =
      "com.github.ambry.cloud.azure.AzureCloudDestinationFactory";
  private static final String DEFAULT_VCR_CLUSTER_ZK_CONNECT_STRING = "localhost:2181";
  private static final String DEFAULT_VCR_CLUSTER_NAME = "VCRCluster";
  private static final String DEFAULT_VCR_REPLICA_MOUNT_PATH_PREFIX = "/tmp/vcr/";
  private static final int DEFAULT_MIN_TTL_DAYS = 14;

  /**
   * The virtual replicator cluster factory class name.
   */
  @Config(VIRTUAL_REPLICATOR_CLUSTER_FACTORY_CLASS)
  @Default(DEFAULT_VIRTUAL_REPLICATOR_CLUSTER_FACTORY_CLASS)
  public final String virtualReplicatorClusterFactoryClass;

  /**
   * The cloud destination factory class name.
   */
  @Config(CLOUD_DESTINATION_FACTORY_CLASS)
  @Default(DEFAULT_CLOUD_DESTINATION_FACTORY_CLASS)
  public final String cloudDestinationFactoryClass;

  /**
   * The ZK connect string of the vcr cluster.
   */
  @Config(VCR_CLUSTER_ZK_CONNECT_STRING)
  @Default(DEFAULT_VCR_CLUSTER_ZK_CONNECT_STRING)
  public final String vcrClusterZkConnectString;

  /**
   * The name of the associated vcr cluster for this node.
   */
  @Config(VCR_CLUSTER_NAME)
  @Default(DEFAULT_VCR_CLUSTER_NAME)
  public final String vcrClusterName;

  /**
   * The ssl port number associated with this node.
   */
  @Config(VCR_SSL_PORT)
  @Default("null")
  public final Integer vcrSslPort;

  /**
   * The path prefix for CloudReplica mouth path.
   */
  @Config(VCR_REPLICA_MOUNT_PATH_PREFIX)
  @Default(DEFAULT_VCR_REPLICA_MOUNT_PATH_PREFIX)
  public final String vcrReplicaMountPathPrefix;

  /**
   * Require blobs to be encrypted prior to cloud upload?
   */
  @Config(VCR_REQUIRE_ENCRYPTION)
  @Default("false")
  public final boolean vcrRequireEncryption;

  /**
   * Minimum TTL in days required for blobs to be uploaded to cloud.
   */
  @Config(VCR_MIN_TTL_DAYS)
  @Default("14")
  public final int vcrMinTtlDays;

  /**
   * The comma-separated list of statically assigned partitions.  Optional.
   */
  @Config(VCR_ASSIGNED_PARTITIONS)
  @Default("null")
  public final String vcrAssignedPartitions;

  public CloudConfig(VerifiableProperties verifiableProperties) {

    virtualReplicatorClusterFactoryClass = verifiableProperties.getString(VIRTUAL_REPLICATOR_CLUSTER_FACTORY_CLASS,
        DEFAULT_VIRTUAL_REPLICATOR_CLUSTER_FACTORY_CLASS);
    cloudDestinationFactoryClass =
        verifiableProperties.getString(CLOUD_DESTINATION_FACTORY_CLASS, DEFAULT_CLOUD_DESTINATION_FACTORY_CLASS);
    vcrClusterZkConnectString =
        verifiableProperties.getString(VCR_CLUSTER_ZK_CONNECT_STRING, DEFAULT_VCR_CLUSTER_ZK_CONNECT_STRING);
    vcrClusterName = verifiableProperties.getString(VCR_CLUSTER_NAME, DEFAULT_VCR_CLUSTER_NAME);
    vcrSslPort = verifiableProperties.getInteger(VCR_SSL_PORT, null);
    vcrReplicaMountPathPrefix =
        verifiableProperties.getString(VCR_REPLICA_MOUNT_PATH_PREFIX, DEFAULT_VCR_REPLICA_MOUNT_PATH_PREFIX);
    vcrRequireEncryption = verifiableProperties.getBoolean(VCR_REQUIRE_ENCRYPTION, false);
    vcrMinTtlDays = verifiableProperties.getInt(VCR_MIN_TTL_DAYS, DEFAULT_MIN_TTL_DAYS);
    vcrAssignedPartitions = verifiableProperties.getString(VCR_ASSIGNED_PARTITIONS, null);
  }
}
