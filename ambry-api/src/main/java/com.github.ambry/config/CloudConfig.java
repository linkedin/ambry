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

  private final static String VCR_CLUSTER_NAME = "VCRCluster";
  private final static String VCR_CLUSTER_ZK_CONNECT_STRING = "localhost:2181";
  private final static String VCR_REPLICA_MOUNT_PATH_PREFIX = "/tmp/vcr/";
  public static final String CLOUD_DESTINATION_FACTORY_CLASS = "cloud.destination.factory.class";
  public static final String DEFAULT_CLOUD_DESTINATION_FACTORY_CLASS =
      "com.github.ambry.cloud.azure.AzureCloudDestinationFactory";
  public static final String CRYPTO_SERVICE_FACTORY_CLASS = "vcr.crypto.service.factory.class";
  public static final String DEFAULT_CRYPTO_SERVICE_FACTORY_CLASS = "com.github.ambry.router.GCMCryptoServiceFactory";
  public static final String KMS_SERVICE_FACTORY_CLASS = "vcr.kms.service.factory.class";
  public static final String DEFAULT_KMS_SERVICE_FACTORY_CLASS =
      "com.github.ambry.router.SingleKeyManagementServiceFactory";
  public static final String KMS_SERVICE_KEY_CONTEXT = "vcr.kms.service.key.context";
  public static final String DEFAULT_KMS_SERVICE_KEY_CONTEXT = "vcr.kms.service.key.context";
  public static final String CLOUD_BLOB_CRYPTO_SERVICE_FACTORY_CLASS = "vcr.cloud.blob.crypto.service.factory.class";
  public static final String DEFAULT_CLOUD_BLOB_CRYPTO_SERVICE_FACTORY_CLASS =
      "com.github.ambry.cloud.azure.AzureCloudBlobCryptoServiceFactory";

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
  @Default("6668")
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
   * The crypto service factory class name.
   */
  @Config(CRYPTO_SERVICE_FACTORY_CLASS)
  @Default(DEFAULT_CRYPTO_SERVICE_FACTORY_CLASS)
  public final String cryptoServiceFactoryClass;

  /**
   * The kms service factory class name.
   */
  @Config(KMS_SERVICE_FACTORY_CLASS)
  @Default(DEFAULT_KMS_SERVICE_FACTORY_CLASS)
  public final String kmsServiceFactoryClass;

  /**
   * The kms destination factory class name.
   */
  @Config(KMS_SERVICE_KEY_CONTEXT)
  @Default(DEFAULT_KMS_SERVICE_KEY_CONTEXT)
  public final String kmsServiceKeyContext;

  /**
   * The cloud blob crypto service factory class name.
   */
  @Config(CLOUD_BLOB_CRYPTO_SERVICE_FACTORY_CLASS)
  @Default(DEFAULT_CLOUD_BLOB_CRYPTO_SERVICE_FACTORY_CLASS)
  public final String cloudBlobCryptoServiceFactoryClass;

  public CloudConfig(VerifiableProperties verifiableProperties) {

    vcrClusterZkConnectString =
        verifiableProperties.getString("vcr.cluster.zk.connect.string", VCR_CLUSTER_ZK_CONNECT_STRING);
    vcrClusterName = verifiableProperties.getString("vcr.cluster.name", VCR_CLUSTER_NAME);
    vcrSslPort = verifiableProperties.getInteger("vcr.ssl.port", 6668);
    vcrReplicaMountPathPrefix =
        verifiableProperties.getString("vcr.replica.mount.path.prefix", VCR_REPLICA_MOUNT_PATH_PREFIX);
    cloudDestinationFactoryClass =
        verifiableProperties.getString(CLOUD_DESTINATION_FACTORY_CLASS, DEFAULT_CLOUD_DESTINATION_FACTORY_CLASS);
    cryptoServiceFactoryClass =
        verifiableProperties.getString(CRYPTO_SERVICE_FACTORY_CLASS, DEFAULT_CRYPTO_SERVICE_FACTORY_CLASS);
    kmsServiceFactoryClass =
        verifiableProperties.getString(KMS_SERVICE_FACTORY_CLASS, DEFAULT_KMS_SERVICE_FACTORY_CLASS);
    kmsServiceKeyContext = verifiableProperties.getString(KMS_SERVICE_KEY_CONTEXT, DEFAULT_KMS_SERVICE_KEY_CONTEXT);
    cloudBlobCryptoServiceFactoryClass = verifiableProperties.getString(CLOUD_BLOB_CRYPTO_SERVICE_FACTORY_CLASS,
        DEFAULT_CLOUD_BLOB_CRYPTO_SERVICE_FACTORY_CLASS);
  }
}
