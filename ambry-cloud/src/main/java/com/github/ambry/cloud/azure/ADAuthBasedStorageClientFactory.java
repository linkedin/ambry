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

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.core.util.Configuration;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import com.github.ambry.config.CloudConfig;
import com.microsoft.aad.msal4j.ClientCredentialFactory;
import com.microsoft.aad.msal4j.ClientCredentialParameters;
import com.microsoft.aad.msal4j.ConfidentialClientApplication;
import com.microsoft.aad.msal4j.IAuthenticationResult;
import java.net.MalformedURLException;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;


/**
 * {@link StorageClientFactory} implementation for AD based authentication.
 */
public class ADAuthBasedStorageClientFactory implements StorageClientFactory {
  private static final Logger logger = LoggerFactory.getLogger(ADAuthBasedStorageClientFactory.class);

  @Override
  public BlobServiceClient createBlobStorageClient(CloudConfig cloudConfig, AzureCloudConfig azureCloudConfig)
      throws MalformedURLException, InterruptedException, ExecutionException {
    Configuration storageConfiguration = new Configuration();

    // Note: retry decisions are made at CloudBlobStore level.  Configure storageClient with no retries.
    RequestRetryOptions noRetries = new RequestRetryOptions(RetryPolicyType.FIXED, 1, null, null, null, null);
    IAuthenticationResult iAuthenticationResult = getAccessTokenByClientCredentialGrant(azureCloudConfig);
    BlobServiceClient storageClient = new BlobServiceClientBuilder().credential(new TokenCredential() {
      @Override
      public Mono<AccessToken> getToken(TokenRequestContext request) {
        return Mono.just(new AccessToken(iAuthenticationResult.accessToken(),
            iAuthenticationResult.expiresOnDate().toInstant().atOffset(OffsetDateTime.now().getOffset())));
      }
    })
        .endpoint(azureCloudConfig.azureStorageEndpoint)
        .retryOptions(noRetries)
        .configuration(storageConfiguration)
        .buildClient();
    return storageClient;
  }

  /**
   * Create {@link IAuthenticationResult} using the app details.
   * @param azureCloudConfig {@link AzureCloudConfig} object.
   * @return {@link IAuthenticationResult} containing the access token.
   * @throws MalformedURLException
   * @throws InterruptedException
   * @throws ExecutionException
   */
  private IAuthenticationResult getAccessTokenByClientCredentialGrant(AzureCloudConfig azureCloudConfig)
      throws MalformedURLException, InterruptedException, ExecutionException {

    ConfidentialClientApplication app = ConfidentialClientApplication.builder(azureCloudConfig.azureStorageClientId,
        ClientCredentialFactory.createFromSecret(azureCloudConfig.azureStorageSecret))
        .authority(azureCloudConfig.azureStorageAuthority)
        .build();

    ClientCredentialParameters clientCredentialParam =
        ClientCredentialParameters.builder(Collections.singleton(azureCloudConfig.azureStorageScope)).build();

    CompletableFuture<IAuthenticationResult> future = app.acquireToken(clientCredentialParam);
    return future.get();
  }
}
