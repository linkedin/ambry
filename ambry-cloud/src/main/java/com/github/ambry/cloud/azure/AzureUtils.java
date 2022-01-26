/*
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import java.net.InetSocketAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utils for working with various Azure services
 */
class AzureUtils {
  private static final Logger logger = LoggerFactory.getLogger(AzureUtils.class);

  /**
   * Check that required configs are present for constructing a {@link ClientSecretCredential}.
   * @param azureCloudConfig the configs.
   */
  static void validateAzureIdentityConfigs(AzureCloudConfig azureCloudConfig) {
    if (azureCloudConfig.azureIdentityTenantId.isEmpty() || azureCloudConfig.azureIdentityClientId.isEmpty()
        || azureCloudConfig.azureIdentitySecret.isEmpty()) {
      throw new IllegalArgumentException(
          String.format("One of the required configs for using ClientSecretCredential (%s, %s, %s) is missing",
              AzureCloudConfig.AZURE_IDENTITY_TENANT_ID, AzureCloudConfig.AZURE_IDENTITY_CLIENT_ID,
              AzureCloudConfig.AZURE_IDENTITY_SECRET));
    }
  }

  /**
   * @param azureCloudConfig the {@link AzureCloudConfig} to source credential configs from.
   * @return the {@link ClientSecretCredential}.
   */
  static ClientSecretCredential getClientSecretCredential(AzureCloudConfig azureCloudConfig) {
    ClientSecretCredentialBuilder builder =
        new ClientSecretCredentialBuilder().tenantId(azureCloudConfig.azureIdentityTenantId)
            .clientId(azureCloudConfig.azureIdentityClientId)
            .clientSecret(azureCloudConfig.azureIdentitySecret);
    if (!azureCloudConfig.azureIdentityProxyHost.isEmpty()) {
      logger.info("Using proxy for ClientSecretCredential: {}:{}", azureCloudConfig.azureIdentityProxyHost,
          azureCloudConfig.azureIdentityProxyPort);
      ProxyOptions proxyOptions = new ProxyOptions(ProxyOptions.Type.HTTP,
          new InetSocketAddress(azureCloudConfig.azureIdentityProxyHost, azureCloudConfig.azureIdentityProxyPort));
      HttpClient httpClient = new NettyAsyncHttpClientBuilder().proxy(proxyOptions).build();
      builder.httpClient(httpClient);
    }
    return builder.build();
  }
}
