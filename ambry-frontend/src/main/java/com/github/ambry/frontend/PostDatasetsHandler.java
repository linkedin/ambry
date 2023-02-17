/*
 * Copyright 2023 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.frontend;

import com.github.ambry.account.AccountCollectionSerde;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.AccountServiceException;
import com.github.ambry.account.Dataset;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.Callback;
import com.github.ambry.commons.RetainingAsyncWritableChannel;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestRequestMetrics;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.ReadableStreamChannel;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.GregorianCalendar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.frontend.FrontendUtils.*;
import static com.github.ambry.frontend.Operations.*;


/**
 * Handle requests to create or update dataset using {@link AccountService}.
 */
public class PostDatasetsHandler {
  private static final Logger logger = LoggerFactory.getLogger(PostDatasetsHandler.class);
  private final SecurityService securityService;
  private final AccountService accountService;
  private final FrontendConfig frontendConfig;
  private final FrontendMetrics frontendMetrics;
  private final AccountAndContainerInjector accountAndContainerInjector;

  /**
   * Constructs a handler for handling requests creating or updating dataset.
   * @param securityService the {@link SecurityService} to use.
   * @param accountService the {@link AccountService} to use.
   * @param frontendConfig the {@link FrontendConfig} to use.
   * @param frontendMetrics {@link FrontendMetrics} instance where metrics should be recorded.
   * @param accountAndContainerInjector helper to resolve account and container for a given request.
   */
  PostDatasetsHandler(SecurityService securityService, AccountService accountService, FrontendConfig frontendConfig,
      FrontendMetrics frontendMetrics, AccountAndContainerInjector accountAndContainerInjector) {
    this.securityService = securityService;
    this.accountService = accountService;
    this.frontendConfig = frontendConfig;
    this.frontendMetrics = frontendMetrics;
    this.accountAndContainerInjector = accountAndContainerInjector;
  }

  /**
   * Asynchronously update account metadata.
   * @param restRequest the {@link RestRequest} that contains the request parameters and body.
   * @param restResponseChannel the {@link RestResponseChannel} where headers should be set.
   * @param callback the {@link Callback} to invoke when the response is ready (or if there is an exception).
   */
  void handle(RestRequest restRequest, RestResponseChannel restResponseChannel,
      Callback<ReadableStreamChannel> callback) {
    new CallbackChain(restRequest, restResponseChannel, callback).start();
  }

  /**
   * Represents the chain of actions to take. Keeps request context that is relevant to all callback stages.
   */
  private class CallbackChain {
    private final RestRequest restRequest;
    private final String uri;
    private final RestResponseChannel restResponseChannel;
    private final Callback<ReadableStreamChannel> finalCallback;
    private Dataset datasetToUpdate;

    /**
     * @param restRequest the {@link RestRequest}.
     * @param restResponseChannel the {@link RestResponseChannel}.
     * @param finalCallback the {@link Callback} to call on completion.
     */
    private CallbackChain(RestRequest restRequest, RestResponseChannel restResponseChannel,
        Callback<ReadableStreamChannel> finalCallback) {
      this.restRequest = restRequest;
      this.restResponseChannel = restResponseChannel;
      this.finalCallback = finalCallback;
      this.uri = restRequest.getUri();
    }

    /**
     * Start the chain by calling {@link SecurityService#processRequest}.
     */
    private void start() {
      RestRequestMetrics requestMetrics =
          frontendMetrics.postDatasetsMetricsGroup.getRestRequestMetrics(restRequest.isSslUsed(), false);
      restRequest.getMetricsTracker().injectMetrics(requestMetrics);
      RetainingAsyncWritableChannel channel = new RetainingAsyncWritableChannel(frontendConfig.maxJsonRequestSizeBytes);
      restRequest.readInto(channel, fetchDatasetUpdateBodyCallback(channel));
    }

    /**
     * After reading the body of the account update request, inject the account and container in request for access check.
     * @param channel the {@link RetainingAsyncWritableChannel} to read data out of.
     * @return a {@link Callback} to be used with {@link RestRequest#readInto}.
     */
    private Callback<Long> fetchDatasetUpdateBodyCallback(RetainingAsyncWritableChannel channel) {
      return buildCallback(frontendMetrics.postDatasetsReadRequestMetrics, bytesRead -> {
        try {
          datasetToUpdate = AccountCollectionSerde.datasetsFromInputStreamInJson(channel.consumeContentAsInputStream());
          if (datasetToUpdate == null) {
            throw new RestServiceException("The dataset is null after deserialize from given InputStream",
                RestServiceErrorCode.BadRequest);
          }
          accountAndContainerInjector.injectAccountAndContainerUsingDatasetBody(restRequest, datasetToUpdate);
          // Start the callback chain by performing request security processing.
          securityService.processRequest(restRequest, securityProcessRequestCallback());
        } catch (IOException e) {
          throw new RestServiceException("Bad dataset update request body", e, RestServiceErrorCode.BadRequest);
        }
      }, uri, logger, finalCallback);
    }

    /**
     * After {@link SecurityService#processRequest} finishes, call {@link SecurityService#postProcessRequest} to perform
     * request time security checks that rely on the request being fully parsed and any additional arguments set.
     * @return a {@link Callback} to be used with {@link SecurityService#processRequest}.
     */
    private Callback<Void> securityProcessRequestCallback() {
      return buildCallback(frontendMetrics.postDatasetsSecurityProcessRequestMetrics,
          securityCheckResult -> securityService.postProcessRequest(restRequest, securityPostProcessRequestCallback()),
          uri, logger, finalCallback);
    }

    /**
     * After {@link SecurityService#postProcessRequest} finishes, add or update the dataset.
     * @return a {@link Callback} to be used with {@link SecurityService#postProcessRequest}.
     */
    private Callback<Void> securityPostProcessRequestCallback() {
      return buildCallback(frontendMetrics.postDatasetsSecurityPostProcessRequestMetrics, bytesRead -> {
        ReadableStreamChannel outputChannel;
        logger.debug("Got request for {} with payload", ACCOUNTS_CONTAINERS_DATASETS);
        addOrUpdateDataset();
        outputChannel = new ByteBufferReadableStreamChannel(ByteBuffer.allocate(0));
        restResponseChannel.setHeader(RestUtils.Headers.DATE, new GregorianCalendar().getTime());
        restResponseChannel.setHeader(RestUtils.Headers.CONTENT_TYPE, RestUtils.JSON_CONTENT_TYPE);
        restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, outputChannel.getSize());
        finalCallback.onCompletion(outputChannel, null);
      }, uri, logger, finalCallback);
    }

    /**
     * Process the request json and call {@link AccountService#addDataset} or {@link AccountService#updateDataset}
     * to add or update the dataset.
     * @throws RestServiceException
     */
    private void addOrUpdateDataset() throws RestServiceException {
      String accountName = null;
      String containerName = null;
      String datasetName = null;
      try {
        // version schema and expiration time has to be provided during dataset creation.
        if (!RestUtils.isDatasetUpdate(restRequest.getArgs())) {
          if (datasetToUpdate.getVersionSchema() == null) {
            throw new RestServiceException("Version schema can't be null for dataset creation",
                RestServiceErrorCode.BadRequest);
          }
          if (datasetToUpdate.getExpirationTimeMs() == null) {
            throw new RestServiceException("ExpirationTimeMs can't be null for dataset creation",
                RestServiceErrorCode.BadRequest);
          }
        }
        datasetName = datasetToUpdate.getDatasetName();
        accountName = datasetToUpdate.getAccountName();
        containerName = datasetToUpdate.getContainerName();
        if (RestUtils.isDatasetUpdate(restRequest.getArgs())) {
          accountService.updateDataset(datasetToUpdate);
        } else {
          accountService.addDataset(datasetToUpdate);
        }
        restResponseChannel.setHeader(RestUtils.Headers.TARGET_ACCOUNT_NAME, accountName);
        restResponseChannel.setHeader(RestUtils.Headers.TARGET_CONTAINER_NAME, containerName);
        restResponseChannel.setHeader(RestUtils.Headers.TARGET_DATASET_NAME, datasetName);
      } catch (AccountServiceException ex) {
        throw new RestServiceException(
            "Dataset update failed for accountName " + accountName + " containerName " + containerName + " datasetName "
                + datasetName, RestServiceErrorCode.getRestServiceErrorCode(ex.getErrorCode()));
      }
    }
  }
}
