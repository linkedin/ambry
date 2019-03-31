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

  public static final String CLOUD_DESTINATION_FACTORY_CLASS = "cloud.destination.factory.class";
  public static final String VCR_ASSIGNED_PARTITIONS = "vcr.assigned.partitions";
  public static final String VCR_REQUIRE_ENCRYPTION = "vcr.require.encryption";
  public static final String VCR_MIN_TTL_DAYS = "vcr.min.ttl.days";

  private static final String VCR_CLUSTER_NAME = "VCRCluster";
  private static final String VCR_CLUSTER_ZK_CONNECT_STRING = "localhost:2181";
  private static final String VCR_REPLICA_MOUNT_PATH_PREFIX = "/tmp/vcr/";
  private static final String DEFAULT_CLOUD_DESTINATION_FACTORY_CLASS =
      "com.github.ambry.cloud.azure.AzureCloudDestinationFactory";
  private static final int DEFAULT_MIN_TTL_DAYS = 14;

  @Config("vcr.cluster.zk.connect.string")
  @Default(VCR_CLUSTER_ZK_CONNECT_STRING)
  public final String vcrClusterZkConnectString;

  /**
   * The name of the associated vcr cluster for this node.
   */
  @Config("vcr.cluster.name")
  @Default(VCR_CLUSTER_NAME)
  public final String vcrClusterName;

  /**
   * The ssl port number associated with this node.
   */
  @Config("vcr.ssl.port")
  @Default("null")
  public final Integer vcrSslPort;

  /**
   * The path prefix for CloudReplica mouth path.
   */
  @Config("vcr.replica.mount.path.prefix")
  @Default(VCR_REPLICA_MOUNT_PATH_PREFIX)
  public final String vcrReplicaMountPathPrefix;

  /**
   * The cloud destination factory class name.
   */
  @Config(CLOUD_DESTINATION_FACTORY_CLASS)
  @Default(DEFAULT_CLOUD_DESTINATION_FACTORY_CLASS)
  public final String cloudDestinationFactoryClass;

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

    vcrClusterZkConnectString =
        verifiableProperties.getString("vcr.cluster.zk.connect.string", VCR_CLUSTER_ZK_CONNECT_STRING);
    vcrClusterName = verifiableProperties.getString("vcr.cluster.name", VCR_CLUSTER_NAME);
    vcrSslPort = verifiableProperties.getInteger("vcr.ssl.port", null);
    vcrReplicaMountPathPrefix =
        verifiableProperties.getString("vcr.replica.mount.path.prefix", VCR_REPLICA_MOUNT_PATH_PREFIX);
    cloudDestinationFactoryClass =
        verifiableProperties.getString(CLOUD_DESTINATION_FACTORY_CLASS, DEFAULT_CLOUD_DESTINATION_FACTORY_CLASS);
    vcrAssignedPartitions = verifiableProperties.getString(VCR_ASSIGNED_PARTITIONS, null);
    vcrRequireEncryption = verifiableProperties.getBoolean(VCR_REQUIRE_ENCRYPTION, false);
    vcrMinTtlDays = verifiableProperties.getInt(VCR_MIN_TTL_DAYS, DEFAULT_MIN_TTL_DAYS);
  }
}
