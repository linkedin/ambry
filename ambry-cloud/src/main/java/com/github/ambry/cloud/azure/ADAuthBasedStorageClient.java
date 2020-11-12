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
import com.azure.core.http.HttpClient;
import com.azure.core.util.Configuration;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.batch.BlobBatchClient;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.github.ambry.config.CloudConfig;
import com.microsoft.aad.msal4j.ClientCredentialFactory;
import com.microsoft.aad.msal4j.ClientCredentialParameters;
import com.microsoft.aad.msal4j.ConfidentialClientApplication;
import com.microsoft.aad.msal4j.IAuthenticationResult;
import java.net.MalformedURLException;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.http.HttpStatus;
import reactor.core.publisher.Mono;


/**
 * {@link StorageClient} implementation for AD based authentication.
 */
public class ADAuthBasedStorageClient extends StorageClient {
  private AtomicReference<AccessToken> accessTokenRef;

  /**
   * Constructor for {@link ADAuthBasedStorageClient} object.
   * @param cloudConfig {@link CloudConfig} object.
   * @param azureCloudConfig {@link AzureCloudConfig} object.
   * @param azureMetrics {@link AzureMetrics} object.
   * @param blobLayoutStrategy {@link AzureBlobLayoutStrategy} object.
   */
  public ADAuthBasedStorageClient(CloudConfig cloudConfig, AzureCloudConfig azureCloudConfig, AzureMetrics azureMetrics,
      AzureBlobLayoutStrategy blobLayoutStrategy) {
    super(cloudConfig, azureCloudConfig, azureMetrics, blobLayoutStrategy);
  }

  /**
   * Constructor for {@link ADAuthBasedStorageClient} object for testing.
   * @param blobServiceClient {@link BlobServiceClient} object.
   * @param blobBatchClient {@link BlobBatchClient} object.
   * @param azureMetrics {@link AzureMetrics} object.
   * @param blobLayoutStrategy {@link AzureBlobLayoutStrategy} object.
   */
  public ADAuthBasedStorageClient(BlobServiceClient blobServiceClient, BlobBatchClient blobBatchClient,
      AzureMetrics azureMetrics, AzureBlobLayoutStrategy blobLayoutStrategy) {
    super(blobServiceClient, blobBatchClient, azureMetrics, blobLayoutStrategy);
  }

  @Override
  protected BlobServiceClient buildBlobServiceClient(HttpClient httpClient, Configuration configuration,
      RequestRetryOptions retryOptions, AzureCloudConfig azureCloudConfig)
      throws MalformedURLException, InterruptedException, ExecutionException {
    IAuthenticationResult iAuthenticationResult = getAccessTokenByClientCredentialGrant(azureCloudConfig);
    AccessToken accessToken = new AccessToken(iAuthenticationResult.accessToken(),
        iAuthenticationResult.expiresOnDate().toInstant().atOffset(OffsetDateTime.now().getOffset()));
    TokenCredential tokenCredential = new TokenCredential() {
      @Override
      public Mono<AccessToken> getToken(TokenRequestContext request) {
        return Mono.just(accessToken);
      }
    };
    if (accessTokenRef == null) {
      accessTokenRef = new AtomicReference<>(accessToken);
    } else {
      accessTokenRef.set(accessToken);
    }
    return new BlobServiceClientBuilder().credential(tokenCredential)
        .endpoint(azureCloudConfig.azureStorageEndpoint)
        .httpClient(httpClient)
        .retryOptions(retryOptions)
        .configuration(configuration)
        .buildClient();
  }

  @Override
  protected void validateABSAuthConfigs(AzureCloudConfig azureCloudConfig) {
    if (azureCloudConfig.azureStorageAuthority.isEmpty() || azureCloudConfig.azureStorageClientId.isEmpty()
        || azureCloudConfig.azureStorageSecret.isEmpty() || azureCloudConfig.azureStorageEndpoint.isEmpty()
        || azureCloudConfig.azureStorageScope.isEmpty()) {
      throw new IllegalArgumentException(String.format("One of the required configs %s, %s, %s, %s, %s is missing",
          AzureCloudConfig.AZURE_STORAGE_AUTHORITY, AzureCloudConfig.AZURE_STORAGE_CLIENTID,
          AzureCloudConfig.AZURE_STORAGE_ENDPOINT, AzureCloudConfig.AZURE_STORAGE_SECRET,
          AzureCloudConfig.AZURE_STORAGE_SCOPE));
    }
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
    //TODO should proxy be specified while building token?
    ConfidentialClientApplication app = ConfidentialClientApplication.builder(azureCloudConfig.azureStorageClientId,
        ClientCredentialFactory.createFromSecret(azureCloudConfig.azureStorageSecret))
        .authority(azureCloudConfig.azureStorageAuthority)
        .build();
    ClientCredentialParameters clientCredentialParam =
        ClientCredentialParameters.builder(Collections.singleton(azureCloudConfig.azureStorageScope)).build();
    return app.acquireToken(clientCredentialParam).get();
  }

  @Override
  protected boolean handleExceptionAndHintRetry(BlobStorageException blobStorageException) {
    // If the exception has status code 403, refresh the token and create a new storage client with the new token.
    if (blobStorageException.getStatusCode() == HttpStatus.SC_FORBIDDEN) {
      azureMetrics.absForbiddenExceptionCount.inc();
      synchronized (this) {
        // check if the access token has expired before refreshing the token. This is done to prevent multiple threads
        // to attempt token refresh at the same time. It is expected that as a result of token refresh, accessTokenRef
        // will updated with the new token.
        if (accessTokenRef.get().isExpired()) {
          azureMetrics.absTokenRefreshAttemptCount.inc();
          BlobServiceClient blobServiceClient = createBlobStorageClient();
          setClientReferences(blobServiceClient);
          logger.info("Token refresh done.");
        }
      }
      return true;
    }
    return false;
  }
}
