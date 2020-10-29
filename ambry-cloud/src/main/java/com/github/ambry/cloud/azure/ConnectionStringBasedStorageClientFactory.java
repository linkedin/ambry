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
import com.azure.core.http.ProxyOptions;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.core.util.Configuration;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import com.github.ambry.config.CloudConfig;
import java.net.InetSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link StorageClientFactory} implementation based on connection string authentication.
 */
public class ConnectionStringBasedStorageClientFactory implements StorageClientFactory {
  private static final Logger logger = LoggerFactory.getLogger(ConnectionStringBasedStorageClientFactory.class);

  @Override
  public BlobServiceClient createBlobStorageClient(CloudConfig cloudConfig, AzureCloudConfig azureCloudConfig) {
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
    BlobServiceClient storageClient =
        new BlobServiceClientBuilder().connectionString(azureCloudConfig.azureStorageConnectionString)
            .httpClient(client)
            .retryOptions(noRetries)
            .configuration(storageConfiguration)
            .buildClient();
    return storageClient;
  }
}
