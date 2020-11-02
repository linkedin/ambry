/**
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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

import com.azure.core.http.HttpClient;
import com.azure.core.util.Configuration;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.policy.RequestRetryOptions;


/**
 * {@link StorageClientFactory} implementation based on connection string authentication.
 */
public class ConnectionStringBasedStorageClientFactory extends StorageClientFactory {

  @Override
  protected BlobServiceClient buildBlobServiceClient(HttpClient httpClient, Configuration configuration,
      RequestRetryOptions retryOptions, AzureCloudConfig azureCloudConfig) {
    return new BlobServiceClientBuilder().connectionString(azureCloudConfig.azureStorageConnectionString)
        .httpClient(httpClient)
        .retryOptions(retryOptions)
        .configuration(configuration)
        .buildClient();
  }

  /**
   * Validate that all the required configs for connection string based authentication are present.
   * @param azureCloudConfig {@link AzureCloudConfig} object.
   */
  protected void validateABSAuthConfigs(AzureCloudConfig azureCloudConfig) {
    if (azureCloudConfig.azureStorageConnectionString.isEmpty()) {
      throw new IllegalArgumentException(
          "Missing connection string config " + AzureCloudConfig.AZURE_STORAGE_CONNECTION_STRING);
    }
  }
}
