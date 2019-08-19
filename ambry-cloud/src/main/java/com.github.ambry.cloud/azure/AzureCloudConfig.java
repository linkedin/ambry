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

import com.github.ambry.config.Config;
import com.github.ambry.config.Default;
import com.github.ambry.config.VerifiableProperties;


/**
 * The configs for cloud related configurations.
 */
public class AzureCloudConfig {

  public static final String AZURE_STORAGE_CONNECTION_STRING = "azure.storage.connection.string";
  public static final String COSMOS_ENDPOINT = "cosmos.endpoint";
  public static final String COSMOS_COLLECTION_LINK = "cosmos.collection.link";
  public static final String COSMOS_KEY = "cosmos.key";
  public static final String COSMOS_MAX_RETRIES = "cosmos.max.retries";
  public static final String COSMOS_DIRECT_HTTPS = "cosmos.direct.https";
  public static final int DEFAULT_COSMOS_MAX_RETRIES = 5;

  /**
   * The Azure Blob Storage connection string.
   */
  @Config(AZURE_STORAGE_CONNECTION_STRING)
  public final String azureStorageConnectionString;

  /**
   * The Cosmos DB endpoint.
   */
  @Config(COSMOS_ENDPOINT)
  public final String cosmosEndpoint;

  /**
   * The link (URL) for the Cosmos DB metadata collection.
   */
  @Config(COSMOS_COLLECTION_LINK)
  public final String cosmosCollectionLink;

  /**
   * The Cosmos DB connection key.
   */
  @Config(COSMOS_KEY)
  public final String cosmosKey;

  /**
   * The maximum number of retries for Cosmos DB requests.
   */
  @Config(COSMOS_MAX_RETRIES)
  @Default("5")
  public final int cosmosMaxRetries;

  /**
   * Flag indicating whether to use DirectHttps CosmosDB connection mode.
   * Provides better performance but may not work with all firewall settings.
   */
  @Config(COSMOS_DIRECT_HTTPS)
  @Default("false")
  public final boolean cosmosDirectHttps;

  public AzureCloudConfig(VerifiableProperties verifiableProperties) {
    azureStorageConnectionString = verifiableProperties.getString(AZURE_STORAGE_CONNECTION_STRING);
    cosmosEndpoint = verifiableProperties.getString(COSMOS_ENDPOINT);
    cosmosCollectionLink = verifiableProperties.getString(COSMOS_COLLECTION_LINK);
    cosmosKey = verifiableProperties.getString(COSMOS_KEY);
    cosmosMaxRetries = verifiableProperties.getInt(COSMOS_MAX_RETRIES, DEFAULT_COSMOS_MAX_RETRIES);
    cosmosDirectHttps = verifiableProperties.getBoolean(COSMOS_DIRECT_HTTPS, false);
  }
}
