/**
 * Copyright 2020  LinkedIn Corp. All rights reserved.
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
import com.azure.core.http.ProxyOptions;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.core.util.Configuration;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import com.github.ambry.config.CloudConfig;
import com.microsoft.azure.cosmosdb.RetryOptions;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A factory class to create {@link BlobServiceClient} object.
 */
public abstract class StorageClientFactory {
  Logger logger = LoggerFactory.getLogger(StorageClientFactory.class);

  /**
   * Create the {@link BlobServiceClient} object.
   * @param {@link CloudConfig} object.
   * @param {@link AzureCloudConfig} object.
   * @return {@link BlobServiceClient} object.
   */
  public BlobServiceClient createBlobStorageClient(CloudConfig cloudConfig, AzureCloudConfig azureCloudConfig) {
    validateABSAuthConfigs(azureCloudConfig);
    Configuration storageConfiguration = new Configuration();
    // Check for network proxy
    ProxyOptions proxyOptions = (cloudConfig.vcrProxyHost == null) ? null : new ProxyOptions(ProxyOptions.Type.HTTP,
        new InetSocketAddress(cloudConfig.vcrProxyHost, cloudConfig.vcrProxyPort));
    if (proxyOptions != null) {
      logger.info("Using proxy: {}:{}", cloudConfig.vcrProxyHost, cloudConfig.vcrProxyPort);
    }
    HttpClient client = new NettyAsyncHttpClientBuilder().proxy(proxyOptions).build();

    // Note: retry decisions are made at CloudBlobStore level.  Configure storageClient with no retries.
    RequestRetryOptions noRetries = new RequestRetryOptions(RetryPolicyType.FIXED, 1, null, null, null, null);
    try {
      return buildBlobServiceClient(client, storageConfiguration, noRetries, azureCloudConfig);
    } catch (MalformedURLException | InterruptedException | ExecutionException ex) {
      logger.error("Error building ABS blob service client: {}", ex.getMessage());
      throw new IllegalStateException(ex);
    }
  }

  /**
   * Validate that all the required configs for ABS authentication are present.
   * @param azureCloudConfig {@link AzureCloudConfig} object.
   */
  protected abstract void validateABSAuthConfigs(AzureCloudConfig azureCloudConfig);

  /**
   * Build {@link BlobServiceClient}.
   * @param httpClient {@link HttpClient} object.
   * @param configuration {@link Configuration} object.
   * @param retryOptions {@link RetryOptions} object.
   * @param azureCloudConfig {@link AzureCloudConfig} object.
   * @return {@link BlobServiceClient} object.
   */
  protected abstract BlobServiceClient buildBlobServiceClient(HttpClient httpClient, Configuration configuration,
      RequestRetryOptions retryOptions, AzureCloudConfig azureCloudConfig)
      throws MalformedURLException, InterruptedException, ExecutionException;
}
