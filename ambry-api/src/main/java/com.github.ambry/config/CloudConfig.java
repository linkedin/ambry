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

  public static final String CLOUD_IS_VCR = "cloud.is.vcr";
  public static final String VIRTUAL_REPLICATOR_CLUSTER_FACTORY_CLASS = "virtual.replicator.cluster.factory.class";
  public static final String CLOUD_DESTINATION_FACTORY_CLASS = "cloud.destination.factory.class";
  public static final String VCR_CLUSTER_ZK_CONNECT_STRING = "vcr.cluster.zk.connect.string";
  public static final String VCR_CLUSTER_NAME = "vcr.cluster.name";
  public static final String VCR_SSL_PORT = "vcr.ssl.port";
  public static final String VCR_REQUIRE_ENCRYPTION = "vcr.require.encryption";
  public static final String VCR_KMS_FACTORY = "vcr.key.management.service.factory";
  public static final String VCR_CRYPTO_SERVICE_FACTORY = "vcr.crypto.service.factory";

  public static final String VCR_MIN_TTL_DAYS = "vcr.min.ttl.days";
  public static final String CLOUD_DELETED_BLOB_RETENTION_DAYS = "cloud.deleted.blob.retention.days";
  public static final String CLOUD_BLOB_COMPACTION_INTERVAL_HOURS = "cloud.blob.compaction.interval.hours";
  public static final String CLOUD_BLOB_COMPACTION_QUERY_LIMIT = "cloud.blob.compaction.query.limit";
  public static final String CLOUD_RECENT_BLOB_CACHE_LIMIT = "cloud.recent.blob.cache.limit";
  public static final String CLOUD_MAX_ATTEMPTS = "cloud.max.attempts";
  public static final String CLOUD_DEFAULT_RETRY_DELAY = "cloud.default.retry.delay";
  public static final String VCR_ASSIGNED_PARTITIONS = "vcr.assigned.partitions";
  public static final String VCR_PROXY_HOST = "vcr.proxy.host";
  public static final String VCR_PROXY_PORT = "vcr.proxy.port";
  public static final String VCR_CLUSTER_SPECTATOR_FACTORY_CLASS = "vcr.cluster.spectator.factory.class";

  public static final String DEFAULT_VIRTUAL_REPLICATOR_CLUSTER_FACTORY_CLASS =
      "com.github.ambry.cloud.StaticVcrClusterFactory";
  public static final String DEFAULT_CLOUD_DESTINATION_FACTORY_CLASS =
      "com.github.ambry.cloud.azure.AzureCloudDestinationFactory";
  public static final String KMS_SERVICE_KEY_CONTEXT = "kms.service.key.context";
  public static final String DEFAULT_KMS_SERVICE_KEY_CONTEXT = "default-backup";
  public static final String CLOUD_BLOB_CRYPTO_AGENT_FACTORY_CLASS = "cloud.blob.crypto.agent.factory.class";
  public static final String DEFAULT_CLOUD_BLOB_CRYPTO_AGENT_FACTORY_CLASS =
      "com.github.ambry.cloud.CloudBlobCryptoAgentFactoryImpl";
  public static final String DEFAULT_VCR_CLUSTER_ZK_CONNECT_STRING = "localhost:2181";
  public static final String DEFAULT_VCR_CLUSTER_NAME = "VCRCluster";
  public static final int DEFAULT_MIN_TTL_DAYS = 14;
  public static final int DEFAULT_RETENTION_DAYS = 7;
  public static final int DEFAULT_COMPACTION_QUERY_LIMIT = 100000;
  public static final int DEFAULT_RECENT_BLOB_CACHE_LIMIT = 10000;
  public static final int DEFAULT_MAX_ATTEMPTS = 3;
  public static final long DEFAULT_RETRY_DELAY_VALUE = 50;
  public static final int DEFAULT_VCR_PROXY_PORT = 3128;
  public static final String DEFAULT_VCR_CLUSTER_SPECTATOR_FACTORY_CLASS =
      "com.github.ambry.clustermap.HelixClusterSpectatorFactory";

  /**
   * True for VCR node, false for live serving node.
   */
  @Config(CLOUD_IS_VCR)
  @Default("false")
  public final boolean cloudIsVcr;

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
   * Require blobs to be encrypted prior to cloud upload?
   */
  @Config(VCR_REQUIRE_ENCRYPTION)
  @Default("false")
  public final boolean vcrRequireEncryption;

  /**
   * The kms service factory class name.
   */
  @Config(KMS_SERVICE_KEY_CONTEXT)
  @Default(DEFAULT_KMS_SERVICE_KEY_CONTEXT)
  public final String kmsServiceKeyContext;

  /**
   * The cloud blob crypto agent factory class name.
   */
  @Config(CLOUD_BLOB_CRYPTO_AGENT_FACTORY_CLASS)
  @Default(DEFAULT_CLOUD_BLOB_CRYPTO_AGENT_FACTORY_CLASS)
  public final String cloudBlobCryptoAgentFactoryClass;

  /**
   * The KeyManagementServiceFactory that will be used to fetch {@link com.github.ambry.router.KeyManagementService}
   */
  @Config(VCR_KMS_FACTORY)
  @Default(RouterConfig.DEFAULT_KMS_FACTORY)
  public final String vcrKeyManagementServiceFactory;

  /**
   * The CryptoServiceFactory that will be used to fetch {@link com.github.ambry.router.CryptoService}
   */
  @Config(VCR_CRYPTO_SERVICE_FACTORY)
  @Default(RouterConfig.DEFAULT_CRYPTO_SERVICE_FACTORY)
  public final String vcrCryptoServiceFactory;

  /**
   * Minimum TTL in days required for blobs to be uploaded to cloud.
   */
  @Config(VCR_MIN_TTL_DAYS)
  @Default("14")
  public final int vcrMinTtlDays;

  /**
   * How long (in days) a blob must be in deleted state before it is compacted.
   */
  @Config(CLOUD_DELETED_BLOB_RETENTION_DAYS)
  @Default("7")
  public final int cloudDeletedBlobRetentionDays;

  /**
   * The result set limit to set on the dead blobs query used in compaction.
   */
  @Config(CLOUD_BLOB_COMPACTION_QUERY_LIMIT)
  @Default("100000")
  public final int cloudBlobCompactionQueryLimit;

  /**
   * The dead blob compaction interval in hours
   */
  @Config(CLOUD_BLOB_COMPACTION_INTERVAL_HOURS)
  @Default("24")
  public final int cloudBlobCompactionIntervalHours;

  /**
   * The max size of recently-accessed blob cache in each cloud blob store.
   */
  @Config(CLOUD_RECENT_BLOB_CACHE_LIMIT)
  @Default("10000")
  public final int recentBlobCacheLimit;

  /**
   * The comma-separated list of statically assigned partitions.
   * Used by static VCR cluster only.
   */
  @Config(VCR_ASSIGNED_PARTITIONS)
  @Default("null")
  public final String vcrAssignedPartitions;

  /**
   * The proxy host, if any, providing network path to cloud services.
   */
  @Config(VCR_PROXY_HOST)
  @Default("null")
  public final String vcrProxyHost;

  /**
   * The proxy port (used only vcrProxyHost is non-null.
   */
  @Config(VCR_PROXY_PORT)
  @Default("3128")
  public final int vcrProxyPort;

  /**
   * The maximum number of attempts for each cloud operation in live serving mode;
   */
  @Config(CLOUD_MAX_ATTEMPTS)
  public final int cloudMaxAttempts;

  /**
   * The default delay in ms between retries of cloud operations.
   */
  @Config(CLOUD_DEFAULT_RETRY_DELAY)
  public final long cloudDefaultRetryDelay;

  /**
   * The class used to instantiate {@link com.github.ambry.clustermap.ClusterSpectatorFactory}
   */
  @Config(VCR_CLUSTER_SPECTATOR_FACTORY_CLASS)
  @Default(DEFAULT_VCR_CLUSTER_SPECTATOR_FACTORY_CLASS)
  public final String vcrClusterSpectatorFactoryClass;

  public CloudConfig(VerifiableProperties verifiableProperties) {

    cloudIsVcr = verifiableProperties.getBoolean(CLOUD_IS_VCR, false);
    virtualReplicatorClusterFactoryClass = verifiableProperties.getString(VIRTUAL_REPLICATOR_CLUSTER_FACTORY_CLASS,
        DEFAULT_VIRTUAL_REPLICATOR_CLUSTER_FACTORY_CLASS);
    cloudDestinationFactoryClass =
        verifiableProperties.getString(CLOUD_DESTINATION_FACTORY_CLASS, DEFAULT_CLOUD_DESTINATION_FACTORY_CLASS);
    vcrAssignedPartitions = verifiableProperties.getString(VCR_ASSIGNED_PARTITIONS, null);
    vcrClusterZkConnectString =
        verifiableProperties.getString(VCR_CLUSTER_ZK_CONNECT_STRING, DEFAULT_VCR_CLUSTER_ZK_CONNECT_STRING);
    vcrClusterName = verifiableProperties.getString(VCR_CLUSTER_NAME, DEFAULT_VCR_CLUSTER_NAME);
    vcrSslPort = verifiableProperties.getInteger(VCR_SSL_PORT, null);
    vcrRequireEncryption = verifiableProperties.getBoolean(VCR_REQUIRE_ENCRYPTION, false);
    vcrKeyManagementServiceFactory = verifiableProperties.getString(VCR_KMS_FACTORY, RouterConfig.DEFAULT_KMS_FACTORY);
    vcrCryptoServiceFactory =
        verifiableProperties.getString(VCR_CRYPTO_SERVICE_FACTORY, RouterConfig.DEFAULT_CRYPTO_SERVICE_FACTORY);
    kmsServiceKeyContext = verifiableProperties.getString(KMS_SERVICE_KEY_CONTEXT, DEFAULT_KMS_SERVICE_KEY_CONTEXT);
    cloudBlobCryptoAgentFactoryClass = verifiableProperties.getString(CLOUD_BLOB_CRYPTO_AGENT_FACTORY_CLASS,
        DEFAULT_CLOUD_BLOB_CRYPTO_AGENT_FACTORY_CLASS);
    vcrMinTtlDays = verifiableProperties.getInt(VCR_MIN_TTL_DAYS, DEFAULT_MIN_TTL_DAYS);
    cloudDeletedBlobRetentionDays =
        verifiableProperties.getInt(CLOUD_DELETED_BLOB_RETENTION_DAYS, DEFAULT_RETENTION_DAYS);
    cloudBlobCompactionIntervalHours = verifiableProperties.getInt(CLOUD_BLOB_COMPACTION_INTERVAL_HOURS, 24);
    cloudBlobCompactionQueryLimit =
        verifiableProperties.getInt(CLOUD_BLOB_COMPACTION_QUERY_LIMIT, DEFAULT_COMPACTION_QUERY_LIMIT);
    recentBlobCacheLimit = verifiableProperties.getInt(CLOUD_RECENT_BLOB_CACHE_LIMIT, DEFAULT_RECENT_BLOB_CACHE_LIMIT);
    cloudMaxAttempts = verifiableProperties.getInt(CLOUD_MAX_ATTEMPTS, DEFAULT_MAX_ATTEMPTS);
    cloudDefaultRetryDelay = verifiableProperties.getLong(CLOUD_DEFAULT_RETRY_DELAY, DEFAULT_RETRY_DELAY_VALUE);
    // Proxy settings
    vcrProxyHost = verifiableProperties.getString(VCR_PROXY_HOST, null);
    vcrProxyPort = verifiableProperties.getInt(VCR_PROXY_PORT, DEFAULT_VCR_PROXY_PORT);

    vcrClusterSpectatorFactoryClass = verifiableProperties.getString(VCR_CLUSTER_SPECTATOR_FACTORY_CLASS,
        DEFAULT_VCR_CLUSTER_SPECTATOR_FACTORY_CLASS);
  }
}
