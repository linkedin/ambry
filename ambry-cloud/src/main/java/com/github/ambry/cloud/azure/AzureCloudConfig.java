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
package com.github.ambry.cloud.azure;

import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.Config;
import com.github.ambry.config.Default;
import com.github.ambry.config.VerifiableProperties;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.json.JSONArray;
import org.json.JSONObject;


/**
 * The configs for cloud related configurations.
 */
public class AzureCloudConfig {

  public static final String AZURE_STORAGE_CONNECTION_STRING = "azure.storage.connection.string";
  public static final String COSMOS_ENDPOINT = "cosmos.endpoint";
  public static final String COSMOS_DATABASE = "cosmos.database";
  public static final String COSMOS_COLLECTION = "cosmos.collection";
  public static final String COSMOS_DELETED_CONTAINER_COLLECTION = "cosmos.deleted.container.collection";
  public static final String COSMOS_KEY = "cosmos.key";
  public static final String COSMOS_KEY_SECRET_NAME = "cosmos.key.secret.name";
  public static final String COSMOS_VAULT_URL = "cosmos.vault.url";
  public static final String COSMOS_DIRECT_HTTPS = "cosmos.direct.https";
  public static final String AZURE_STORAGE_ACCOUNT_INFO = "azure.storage.account.info";
  public static final String AZURE_STORAGE_AUTHORITY = "azure.storage.authority";
  public static final String AZURE_STORAGE_CLIENTID = "azure.storage.clientId";
  public static final String AZURE_STORAGE_SECRET = "azure.storage.secret";
  public static final String AZURE_STORAGE_SCOPE = "azure.storage.scope";
  public static final String AZURE_STORAGE_ENDPOINT = "azure.storage.endpoint";
  public static final String AZURE_IDENTITY_TENANT_ID = "azure.identity.tenant.id";
  public static final String AZURE_IDENTITY_CLIENT_ID = "azure.identity.client.id";
  public static final String AZURE_IDENTITY_SECRET = "azure.identity.secret";
  public static final String AZURE_IDENTITY_PROXY_HOST = "azure.identity.proxy.host";
  public static final String AZURE_IDENTITY_PROXY_PORT = "azure.identity.proxy.port";
  public static final String COSMOS_QUERY_BATCH_SIZE = "cosmos.query.batch.size";
  public static final String COSMOS_CONTAINER_DELETION_BATCH_SIZE = "cosmos.container.deletion.batch.size";
  public static final String COSMOS_REQUEST_CHARGE_THRESHOLD = "cosmos.request.charge.threshold";
  public static final String COSMOS_CONTINUATION_TOKEN_LIMIT_KB = "cosmos.continuation.token.limit.kb";
  public static final String AZURE_PURGE_BATCH_SIZE = "azure.purge.batch.size";
  public static final String COSMOS_PURGE_BATCH_SIZE = "cosmos.purge.batch.size";
  public static final String AZURE_NAME_SCHEME_VERSION = "azure.name.scheme.version";
  public static final String AZURE_BLOB_CONTAINER_STRATEGY = "azure.blob.container.strategy";
  public static final String AZURE_STORAGE_CLIENT_CLASS = "azure.storage.client.class";
  public static final String CONTAINER_COMPACTION_COSMOS_QUERY_LIMIT = "container.compaction.cosmos.query.limit";
  public static final String CONTAINER_COMPACTION_ABS_PURGE_LIMIT = "container.compaction.abs.purge.limit";
  public static final String AZURE_STORAGE_CLIENT_REFRESH_FACTOR = "azure.storage.client.refresh.factor";
  // Per docs.microsoft.com/en-us/rest/api/storageservices/blob-batch
  public static final int MAX_PURGE_BATCH_SIZE = 256;
  public static final int DEFAULT_PURGE_BATCH_SIZE = 100;
  public static final int DEFAULT_QUERY_BATCH_SIZE = 100;
  public static final int DEFAULT_COSMOS_CONTINUATION_TOKEN_LIMIT = 4;
  public static final int DEFAULT_COSMOS_REQUEST_CHARGE_THRESHOLD = 100;
  public static final int DEFAULT_COSMOS_CONTAINER_DELETION_BATCH_SIZE = 100;
  public static final int DEFAULT_CONTAINER_COMPACTION_COSMOS_QUERY_LIMIT = 100;
  public static final int DEFAULT_CONTAINER_COMPACTION_ABS_PURGE_LIMIT = 100;
  public static final double DEFAULT_AZURE_STORAGE_CLIENT_REFRESH_FACTOR = 0.9F;

  public static final int DEFAULT_NAME_SCHEME_VERSION = 0;
  public static final String DEFAULT_CONTAINER_STRATEGY = "Partition";
  public static final String DEFAULT_AZURE_STORAGE_CLIENT_CLASS =
      "com.github.ambry.cloud.azure.ConnectionStringBasedStorageClient";

  public static final String AZURE_STORAGE_ACCOUNT_INFO_STR = "storageAccountInfo";
  public static final String AZURE_STORAGE_ACCOUNT_INFO_NAME = "name";
  public static final String AZURE_STORAGE_ACCOUNT_INFO_PARTITION_RANGE = "partitionRange";
  public static final String AZURE_STORAGE_ACCOUNT_INFO_STORAGE_SCOPE = "storageScope";
  public static final String AZURE_STORAGE_ACCOUNT_INFO_STORAGE_ENDPOINT = "storageEndpoint";
  public static final String AZURE_STORAGE_ACCOUNT_INFO_STORAGE_CONNECTION_STRING = "storageConnectionString";

  /**
   * Stores Azure Storage Account related info for one storage account.
   */
  public static class StorageAccountInfo {
    private final String name;
    private final int partitionRangeStart;
    private final int partitionRangeEnd;
    private final String storageScope;
    private final String storageEndpoint;
    private final String storageConnectionString;

    /**
     * Construct a StorageAccountInfo object with the given parameters.
     * @param name the name of the storage account.
     * @param partitionRangeStart the lower bound (inclusive) of the partition range this storage account covers.
     * @param partitionRangeEnd the upper bound (exclusive) of the partition range this storage account covers.
     * @param storageScope the Azure scope.
     * @param storageEndpoint the Azure end-point.
     * @param storageConnectionString the Azure end-point in the form of connection string.
     */
    StorageAccountInfo(String name, int partitionRangeStart, int partitionRangeEnd, String storageScope, String storageEndpoint, String storageConnectionString) {
      this.name = name;
      this.partitionRangeStart = partitionRangeStart;
      this.partitionRangeEnd = partitionRangeEnd;

      // Used by ADAuthBasedStorageClient
      this.storageScope = storageScope;

      // Used by ADAuthBasedStorageClient and ClientSecretCredentialStorageClient
      this.storageEndpoint = storageEndpoint;

      // Used by ConnectionStringBasedStorageClient
      this.storageConnectionString = storageConnectionString;
    }

    /**
     * @return the name of {@link StorageAccountInfo}.
     */
    public String getName() {
      return name;
    }

    /**
     * @return the beginning of partition range corresponding to {@link StorageAccountInfo}.
     */
    public int getPartitionRangeStart() {
      return partitionRangeStart;
    }

    /**
     * @return the end of partition range corresponding to {@link StorageAccountInfo}.
     */
    public int getPartitionRangeEnd() {
      return partitionRangeEnd;
    }

    /**
     * @return the storage scope of {@link StorageAccountInfo}.
     */
    public String getStorageScope() {
      return storageScope;
    }

    /**
     * @return the storage end-point of {@link StorageAccountInfo}.
     */
    public String getStorageEndpoint() {
      return storageEndpoint;
    }

    /**
     * @return the storage connection string of {@link StorageAccountInfo}.
     */
    public String getStorageConnectionString() { return storageConnectionString; }
  }

  /**
   * Parse storage account info portion of the {@link AzureCloudConfig} and return the list of storage accounts
   * in the form of {@link StorageAccountInfo}.
   * @param storageAccountInfoStr list of storage account info in the form of json string.
   * @return the list of {@link StorageAccountInfo}.
   */
  static List<StorageAccountInfo> parseStorageAccountInfo(String storageAccountInfoStr) {
    if (storageAccountInfoStr.isEmpty()) {
      return Collections.emptyList();
    }
    int prev_partition_upper_bound = 0;
    List<StorageAccountInfo> storageAccountInfoList = new ArrayList<>();
    JSONObject root = new JSONObject(storageAccountInfoStr);
      JSONArray all = root.getJSONArray(AZURE_STORAGE_ACCOUNT_INFO_STR);
      for (int i = 0; i < all.length(); i++) {
        JSONObject entry = all.getJSONObject(i);
        String name = entry.optString(AZURE_STORAGE_ACCOUNT_INFO_NAME);
        if (name.isEmpty()) {
          throw new IllegalArgumentException(String.format("Storage account #%d is missing the name setting", i));
        }
        String storageScope = entry.optString(AZURE_STORAGE_ACCOUNT_INFO_STORAGE_SCOPE);
        String storageEndpoint = entry.optString(AZURE_STORAGE_ACCOUNT_INFO_STORAGE_ENDPOINT);
        String storageConnectionString = entry.optString(AZURE_STORAGE_ACCOUNT_INFO_STORAGE_CONNECTION_STRING);
        String partitionRange = entry.optString(AZURE_STORAGE_ACCOUNT_INFO_PARTITION_RANGE);
        if (partitionRange.isEmpty()) {
          throw new IllegalArgumentException(
              String.format("The partition range for the storage account %s is missing", name));
        }
        Pattern p = Pattern.compile("(\\d+)-(\\d+)");
        Matcher m = p.matcher(partitionRange);
        if (!m.find()) {
          throw new IllegalArgumentException(
              String.format("The partition range %s for the storage account %s is not valid", partitionRange, name));
        }
        final int lower_partition_bound = Integer.parseInt(m.group(1));
        final int upper_partition_bound = Integer.parseInt(m.group(2));

        if (upper_partition_bound - lower_partition_bound < 1) {
          throw new IllegalArgumentException(
              String.format("The partition range %s for the storage account %s is not valid", partitionRange, name));
        }

        if (lower_partition_bound != prev_partition_upper_bound) {
          throw new IllegalArgumentException(
              String.format("The partition range %s for the storage account %s is not valid since it leaves a "
              + "gap which is not covered by other storage accounts", partitionRange, name));
        }
        prev_partition_upper_bound = upper_partition_bound;
        storageAccountInfoList.add(new StorageAccountInfo(name, lower_partition_bound, upper_partition_bound,
            storageScope, storageEndpoint, storageConnectionString));
      }

      return storageAccountInfoList;
  }

  /**
   * The Azure Blob Storage connection string.
   */
  @Config(AZURE_STORAGE_CONNECTION_STRING)
  @Default("")
  public final String azureStorageConnectionString;

  /**
   * The Cosmos DB endpoint.
   */
  @Config(COSMOS_ENDPOINT)
  public final String cosmosEndpoint;

  /**
   * The Cosmos DB database name.
   */
  @Config(COSMOS_DATABASE)
  public final String cosmosDatabase;

  /**
   * The Cosmos DB container/collection for storing metadata of blobs.
   */
  @Config(COSMOS_COLLECTION)
  public final String cosmosCollection;

  /**
   * The Cosmos DB container/collection for storing list of deleted Ambry containers.
   */
  @Config(COSMOS_DELETED_CONTAINER_COLLECTION)
  public final String cosmosDeletedContainerCollection;

  /**
   * The Cosmos DB connection key.
   */
  @Config(COSMOS_KEY)
  @Default("")
  public final String cosmosKey;

  /**
   * The name of the secret in an Azure KeyVault containing the key to connect to Cosmos DB.
   * Used as an alternative to configuring the key directly in {@link #COSMOS_KEY}.
   */
  @Config(COSMOS_KEY_SECRET_NAME)
  @Default("")
  public final String cosmosKeySecretName;

  /**
   * The URL for the Azure KeyVault containing the cosmos key.
   * Used as an alternative to configuring the key directly in {@link #COSMOS_KEY}.
   */
  @Config(COSMOS_VAULT_URL)
  @Default("")
  public final String cosmosVaultUrl;

  @Config(AZURE_PURGE_BATCH_SIZE)
  @Default("100")
  public final int azurePurgeBatchSize;

  @Config(COSMOS_PURGE_BATCH_SIZE)
  public final int cosmosPurgeBatchSize;

  @Config(AZURE_NAME_SCHEME_VERSION)
  @Default("0")
  public final int azureNameSchemeVersion;

  @Config(AZURE_BLOB_CONTAINER_STRATEGY)
  @Default("Partition")
  public final String azureBlobContainerStrategy;

  /**
   * Max number of metadata records to fetch in a single Cosmos query.
   */
  @Config(COSMOS_QUERY_BATCH_SIZE)
  public final int cosmosQueryBatchSize;

  @Config(COSMOS_CONTAINER_DELETION_BATCH_SIZE)
  public final int cosmosContainerDeletionBatchSize;

  /**
   * The size limit in KB on Cosmos continuation token.
   */
  @Config(COSMOS_CONTINUATION_TOKEN_LIMIT_KB)
  public final int cosmosContinuationTokenLimitKb;

  /**
   * The Cosmos request charge threshold to log.
   */
  @Config(COSMOS_REQUEST_CHARGE_THRESHOLD)
  public final int cosmosRequestChargeThreshold;

  /**
   * Flag indicating whether to use DirectHttps CosmosDB connection mode.
   * Provides better performance but may not work with all firewall settings.
   */
  @Config(COSMOS_DIRECT_HTTPS)
  @Default("false")
  public final boolean cosmosDirectHttps;

  /**
   * Azure storage account info.
   */
  @Config(AZURE_STORAGE_ACCOUNT_INFO)
  @Default("")
  public final List<StorageAccountInfo> azureStorageAccountInfo;

  /**
   * Azure storage authority.
   */
  @Config(AZURE_STORAGE_AUTHORITY)
  @Default("")
  public final String azureStorageAuthority;

  /**
   * Azure storage client id.
   */
  @Config(AZURE_STORAGE_CLIENTID)
  @Default("")
  public final String azureStorageClientId;

  /**
   * Azure storage client secret.
   */
  @Config(AZURE_STORAGE_SECRET)
  @Default("")
  public final String azureStorageSecret;

  @Config(AZURE_STORAGE_SCOPE)
  @Default("")
  public final String azureStorageScope;

  /**
   * Azure storage endpoint.
   */
  @Config(AZURE_STORAGE_ENDPOINT)
  @Default("")
  public final String azureStorageEndpoint;

  /**
   * Azure AAD identity tenant id. For use with {@code ClientSecretCredential} auth.
   */
  @Config(AZURE_IDENTITY_TENANT_ID)
  @Default("")
  public final String azureIdentityTenantId;

  /**
   * Azure AAD identity client id. For use with {@code ClientSecretCredential} auth.
   */
  @Config(AZURE_IDENTITY_CLIENT_ID)
  @Default("")
  public final String azureIdentityClientId;

  /**
   * Azure AAD identity client secret. For use with {@code ClientSecretCredential} auth.
   */
  @Config(AZURE_IDENTITY_SECRET)
  @Default("")
  public final String azureIdentitySecret;

  /**
   * Azure AAD identity proxy host. This is a separate config from other services since there are cases where a proxy
   * is required only for AAD (since AAD doesn't support private endpoints).
   * For use with {@code ClientSecretCredential} auth.
   */
  @Config(AZURE_IDENTITY_PROXY_HOST)
  @Default("")
  public final String azureIdentityProxyHost;

  /**
   * Azure AAD identity proxy port. For use with {@code ClientSecretCredential} auth.
   */
  @Config(AZURE_IDENTITY_PROXY_PORT)
  @Default("3128")
  public final int azureIdentityProxyPort;

  /**
   * Factory class to instantiate azure storage client.
   */
  @Config(AZURE_STORAGE_CLIENT_CLASS)
  public final String azureStorageClientClass;

  /*
   *  Number of blobs to fetch from Cosmos db for each container compaction query.
   */
  @Config(CONTAINER_COMPACTION_COSMOS_QUERY_LIMIT)
  public int containerCompactionCosmosQueryLimit;

  /**
   * Number of blobs to purge from ABS in each container compaction purge request.
   */
  @Config(CONTAINER_COMPACTION_ABS_PURGE_LIMIT)
  public int containerCompactionAbsPurgeLimit;

  /**
   * Fraction of token expiry time after which storage client token refresh will be attempted.
   */
  @Config(AZURE_STORAGE_CLIENT_REFRESH_FACTOR)
  public double azureStorageClientRefreshFactor;

  public AzureCloudConfig(VerifiableProperties verifiableProperties) {
    azureStorageConnectionString = verifiableProperties.getString(AZURE_STORAGE_CONNECTION_STRING, "");
    cosmosEndpoint = verifiableProperties.getString(COSMOS_ENDPOINT);
    cosmosDatabase = verifiableProperties.getString(COSMOS_DATABASE);
    cosmosCollection = verifiableProperties.getString(COSMOS_COLLECTION);
    cosmosDeletedContainerCollection = verifiableProperties.getString(COSMOS_DELETED_CONTAINER_COLLECTION, "");
    cosmosKey = verifiableProperties.getString(COSMOS_KEY, "");
    cosmosKeySecretName = verifiableProperties.getString(COSMOS_KEY_SECRET_NAME, "");
    cosmosVaultUrl = verifiableProperties.getString(COSMOS_VAULT_URL, "");
    azureStorageAccountInfo = parseStorageAccountInfo(verifiableProperties.getString(AZURE_STORAGE_ACCOUNT_INFO, ""));
    azureStorageAuthority = verifiableProperties.getString(AZURE_STORAGE_AUTHORITY, "");
    azureStorageClientId = verifiableProperties.getString(AZURE_STORAGE_CLIENTID, "");
    azureStorageSecret = verifiableProperties.getString(AZURE_STORAGE_SECRET, "");
    azureStorageScope = verifiableProperties.getString(AZURE_STORAGE_SCOPE, "");
    azureStorageEndpoint = verifiableProperties.getString(AZURE_STORAGE_ENDPOINT, "");
    azureIdentityTenantId = verifiableProperties.getString(AZURE_IDENTITY_TENANT_ID, "");
    azureIdentityClientId = verifiableProperties.getString(AZURE_IDENTITY_CLIENT_ID, "");
    azureIdentitySecret = verifiableProperties.getString(AZURE_IDENTITY_SECRET, "");
    azureIdentityProxyHost = verifiableProperties.getString(AZURE_IDENTITY_PROXY_HOST, "");
    azureIdentityProxyPort = verifiableProperties.getInt(AZURE_IDENTITY_PROXY_PORT, CloudConfig.DEFAULT_VCR_PROXY_PORT);
    cosmosQueryBatchSize = verifiableProperties.getInt(COSMOS_QUERY_BATCH_SIZE, DEFAULT_QUERY_BATCH_SIZE);
    cosmosContinuationTokenLimitKb =
        verifiableProperties.getInt(COSMOS_CONTINUATION_TOKEN_LIMIT_KB, DEFAULT_COSMOS_CONTINUATION_TOKEN_LIMIT);
    cosmosRequestChargeThreshold =
        verifiableProperties.getInt(COSMOS_REQUEST_CHARGE_THRESHOLD, DEFAULT_COSMOS_REQUEST_CHARGE_THRESHOLD);
    azurePurgeBatchSize =
        verifiableProperties.getIntInRange(AZURE_PURGE_BATCH_SIZE, DEFAULT_PURGE_BATCH_SIZE, 1, MAX_PURGE_BATCH_SIZE);
    cosmosPurgeBatchSize = verifiableProperties.getInt(COSMOS_PURGE_BATCH_SIZE, azurePurgeBatchSize);
    cosmosDirectHttps = verifiableProperties.getBoolean(COSMOS_DIRECT_HTTPS, false);
    azureBlobContainerStrategy =
        verifiableProperties.getString(AZURE_BLOB_CONTAINER_STRATEGY, DEFAULT_CONTAINER_STRATEGY);
    azureNameSchemeVersion = verifiableProperties.getInt(AZURE_NAME_SCHEME_VERSION, DEFAULT_NAME_SCHEME_VERSION);
    azureStorageClientClass =
        verifiableProperties.getString(AZURE_STORAGE_CLIENT_CLASS, DEFAULT_AZURE_STORAGE_CLIENT_CLASS);
    cosmosContainerDeletionBatchSize =
        verifiableProperties.getInt(COSMOS_CONTAINER_DELETION_BATCH_SIZE, DEFAULT_COSMOS_CONTAINER_DELETION_BATCH_SIZE);
    containerCompactionAbsPurgeLimit =
        verifiableProperties.getInt(CONTAINER_COMPACTION_ABS_PURGE_LIMIT, DEFAULT_CONTAINER_COMPACTION_ABS_PURGE_LIMIT);
    containerCompactionCosmosQueryLimit = verifiableProperties.getIntInRange(CONTAINER_COMPACTION_COSMOS_QUERY_LIMIT,
        DEFAULT_CONTAINER_COMPACTION_COSMOS_QUERY_LIMIT, 1, Integer.MAX_VALUE);
    azureStorageClientRefreshFactor = verifiableProperties.getDoubleInRange(AZURE_STORAGE_CLIENT_REFRESH_FACTOR,
        DEFAULT_AZURE_STORAGE_CLIENT_REFRESH_FACTOR, 0.0, 1.0);
  }
}
