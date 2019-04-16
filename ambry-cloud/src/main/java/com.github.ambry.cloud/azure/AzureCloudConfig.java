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
import com.github.ambry.config.VerifiableProperties;


/**
 * The configs for cloud related configurations.
 */
public class AzureCloudConfig {

  public static final String AZURE_STORAGE_CONNECTION_STRING = "azure.storage.connection.string";
  public static final String COSMOS_ENDPOINT = "cosmos.endpoint";
  public static final String COSMOS_COLLECTION_LINK = "cosmos.collection.link";
  public static final String COSMOS_KEY = "cosmos.key";

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

  public AzureCloudConfig(VerifiableProperties verifiableProperties) {
    azureStorageConnectionString = verifiableProperties.getString(AZURE_STORAGE_CONNECTION_STRING);
    cosmosEndpoint = verifiableProperties.getString(COSMOS_ENDPOINT);
    cosmosCollectionLink = verifiableProperties.getString(COSMOS_COLLECTION_LINK);
    cosmosKey = verifiableProperties.getString(COSMOS_KEY);
  }
}
