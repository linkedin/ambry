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
import com.azure.core.http.HttpClient;
import com.azure.core.util.Configuration;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.batch.BlobBatchAsyncClient;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.utils.Utils;
import com.microsoft.aad.msal4j.ClientCredentialFactory;
import com.microsoft.aad.msal4j.ClientCredentialParameters;
import com.microsoft.aad.msal4j.ConfidentialClientApplication;
import com.microsoft.aad.msal4j.IAuthenticationResult;
import com.mysql.cj.util.StringUtils;
import java.net.MalformedURLException;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.http.HttpStatus;
import reactor.core.publisher.Mono;

import static com.github.ambry.cloud.azure.AzureCloudConfig.*;


/**
 * {@link StorageClient} implementation for AD based authentication.
 */
public class ADAuthBasedStorageClient extends StorageClient {
  private static final String AD_AUTH_TOKEN_REFRESHER_PREFIX = "AdAuthTokenRefresher";
  private final ScheduledExecutorService tokenRefreshScheduler =
      Utils.newScheduler(1, AD_AUTH_TOKEN_REFRESHER_PREFIX, false);
  private final AtomicReference<ScheduledFuture<?>> scheduledFutureRef = new AtomicReference<>(null);
  private AtomicReference<AccessToken> accessTokenRef;

  /**
   * Constructor for {@link ADAuthBasedStorageClient} object.
   * @param cloudConfig {@link CloudConfig} object.
   * @param azureCloudConfig {@link AzureCloudConfig} object.
   * @param azureMetrics {@link AzureMetrics} object.
   * @param blobLayoutStrategy {@link AzureBlobLayoutStrategy} object.
   */
  public ADAuthBasedStorageClient(CloudConfig cloudConfig, AzureCloudConfig azureCloudConfig, AzureMetrics azureMetrics,
      AzureBlobLayoutStrategy blobLayoutStrategy, AzureCloudConfig.StorageAccountInfo storageAccountInfo) {
    super(cloudConfig, azureCloudConfig, azureMetrics, blobLayoutStrategy, storageAccountInfo);
    // schedule a task to refresh token and create new storage sync and async clients before it expires.
    scheduledFutureRef.set(tokenRefreshScheduler.schedule(() -> refreshTokenAndStorageClients(),
        (long) ((accessTokenRef.get().getExpiresAt().toEpochSecond() - OffsetDateTime.now().toEpochSecond())
            * azureCloudConfig.azureStorageClientRefreshFactor), TimeUnit.SECONDS));
  }

  /**
   * Constructor for {@link ADAuthBasedStorageClient} object for testing.
   * @param blobServiceAsyncClient {@link BlobServiceAsyncClient} object.
   * @param blobBatchAsyncClient {@link BlobBatchAsyncClient} object.
   * @param azureMetrics {@link AzureMetrics} object.
   * @param blobLayoutStrategy {@link AzureBlobLayoutStrategy} object.
   * @param azureCloudConfig {@link AzureCloudConfig} object.
   * @param accessToken {@link AccessToken} object.
   */
  public ADAuthBasedStorageClient(BlobServiceAsyncClient blobServiceAsyncClient,
      BlobBatchAsyncClient blobBatchAsyncClient, AzureMetrics azureMetrics, AzureBlobLayoutStrategy blobLayoutStrategy,
      AzureCloudConfig azureCloudConfig, AccessToken accessToken, AzureCloudConfig.StorageAccountInfo storageAccountInfo) {
    super(blobServiceAsyncClient, blobBatchAsyncClient, azureMetrics, blobLayoutStrategy, azureCloudConfig, storageAccountInfo);
    accessTokenRef = new AtomicReference<>(accessToken);
  }

  @Override
  protected BlobServiceAsyncClient buildBlobServiceAsyncClient(HttpClient httpClient, Configuration configuration,
      RequestRetryOptions retryOptions, AzureCloudConfig azureCloudConfig)
      throws MalformedURLException, InterruptedException, ExecutionException {
    if (accessTokenRef == null) {
      // This means the token is not yet created because we are building storage client for the first time from
      //base class's constructor. Create token before building storage client.
      refreshToken();
    }
    return new BlobServiceClientBuilder().credential(request -> Mono.just(accessTokenRef.get()))
        .endpoint(storageAccountInfo() != null ? storageAccountInfo().getStorageEndpoint() : azureCloudConfig.azureStorageEndpoint)
        .httpClient(httpClient)
        .retryOptions(retryOptions)
        .configuration(configuration)
        .buildAsyncClient();
  }

  @Override
  protected void validateABSAuthConfigs(AzureCloudConfig azureCloudConfig) {
    if (storageAccountInfo() != null) {
      if (StringUtils.isNullOrEmpty(storageAccountInfo().getStorageScope())) {
        throw new IllegalArgumentException(String.format("Storage account %s is missing the %s setting",
            storageAccountInfo().getName(), AZURE_STORAGE_ACCOUNT_INFO_STORAGE_SCOPE));
      }
      if (StringUtils.isNullOrEmpty(storageAccountInfo().getStorageEndpoint())) {
        throw new IllegalArgumentException(
            String.format("Storage account %s is missing the %s setting", storageAccountInfo().getName(),
                AZURE_STORAGE_ACCOUNT_INFO_STORAGE_ENDPOINT));
      }
      return;
    }
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
    // If a proxy is required, properties must either be set at the jvm level,
    // or ClientSecretCredentialStorageClient should be used
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
          refreshTokenAndStorageClients();
        }
      }
      return true;
    }
    return false;
  }

  /**
   * Refreshes AD authentication token and creates a new ABS client with the new token. Also, it schedules a task
   * for repeating this step before the obtained token expires.
   */
  private synchronized void refreshTokenAndStorageClients() {
    azureMetrics.absTokenRefreshAttemptCount.inc();
    try {
      // if a task is already scheduled to refresh token then remove it.
      if (scheduledFutureRef.get() != null) {
        scheduledFutureRef.get().cancel(false);
        scheduledFutureRef.set(null);
      }

      // Refresh Token
      refreshToken();

      // Create new async storage clients with refreshed token.
      BlobServiceAsyncClient blobServiceAsyncClient = createBlobStorageAsyncClient();
      setAsyncClientReferences(blobServiceAsyncClient);

      logger.info("Token refresh done.");

      // schedule a task to refresh token again and create new storage sync and async clients before it expires.
      scheduledFutureRef.set(tokenRefreshScheduler.schedule(this::refreshTokenAndStorageClients,
          (long) ((accessTokenRef.get().getExpiresAt().toEpochSecond() - OffsetDateTime.now().toEpochSecond())
              * azureCloudConfig.azureStorageClientRefreshFactor), TimeUnit.SECONDS));
    } catch (MalformedURLException | InterruptedException | ExecutionException ex) {
      logger.error("Error building ABS blob service client: {}", ex.getMessage());
      throw new IllegalStateException(ex);
    }
  }

  private void refreshToken() throws MalformedURLException, ExecutionException, InterruptedException {
    IAuthenticationResult iAuthenticationResult = getAccessTokenByClientCredentialGrant(azureCloudConfig);
    AccessToken accessToken = new AccessToken(iAuthenticationResult.accessToken(),
        iAuthenticationResult.expiresOnDate().toInstant().atOffset(OffsetDateTime.now().getOffset()));
    if (accessTokenRef == null) {
      accessTokenRef = new AtomicReference<>(accessToken);
    } else {
      accessTokenRef.set(accessToken);
    }
  }
}
