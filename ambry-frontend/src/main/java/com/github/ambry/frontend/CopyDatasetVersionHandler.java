/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.account.AccountService;
import com.github.ambry.account.AccountServiceException;
import com.github.ambry.commons.Callback;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestRequestMetrics;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.ReadableStreamChannel;
import java.util.GregorianCalendar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.frontend.FrontendUtils.*;


public class CopyDatasetVersionHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(CopyDatasetVersionHandler.class);
  private final SecurityService securityService;
  private final AccountAndContainerInjector accountAndContainerInjector;
  private final FrontendMetrics frontendMetrics;
  private final AccountService accountService;

  /**
   * Constructs a handler for handling requests for copy dataset versions under a dataset.
   * @param securityService the {@link SecurityService} to use.
   * @param accountService the {@link AccountService} to use.
   * @param frontendMetrics {@link FrontendMetrics} instance where metrics should be recorded.
   * @param accountAndContainerInjector helper to resolve account and container for a given request.
   */

  CopyDatasetVersionHandler(SecurityService securityService, AccountService accountService,
      FrontendMetrics frontendMetrics, AccountAndContainerInjector accountAndContainerInjector) {
    this.securityService = securityService;
    this.accountService = accountService;
    this.frontendMetrics = frontendMetrics;
    this.accountAndContainerInjector = accountAndContainerInjector;
  }

  void handle(RestRequest restRequest, RestResponseChannel restResponseChannel,
      Callback<ReadableStreamChannel> callback) throws RestServiceException {
    RestRequestMetrics requestMetrics =
        frontendMetrics.copyBlobMetricsGroup.getRestRequestMetrics(restRequest.isSslUsed(), false);
    restRequest.getMetricsTracker().injectMetrics(requestMetrics);
    // copy dataset request have their account/container name in request header, so checks can be done at early stage.
    accountAndContainerInjector.injectAccountContainerForNamedBlob(restRequest, frontendMetrics.copyBlobMetricsGroup);
    new CopyDatasetVersionHandler.CallbackChain(restRequest, restResponseChannel, callback).start();
  }

  /**
   * Represents the chain of actions to take. Keeps request context that is relevant to all callback stages.
   */

  private class CallbackChain {
    private final RestRequest restRequest;
    private final String uri;
    private final RestResponseChannel restResponseChannel;
    private final Callback<ReadableStreamChannel> finalCallback;

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
     * Start the chain by calling {@link SecurityService#preProcessRequest}.
     */

    private void start() {
      // Start the callback chain by performing request security processing.
      securityService.processRequest(restRequest, securityProcessRequestCallback());
    }

    /**
     * After {@link SecurityService#processRequest} finishes, call {@link SecurityService#postProcessRequest} to perform
     * request time security checks that rely on the request being fully parsed and any additional arguments set.
     * @return a {@link Callback} to be used with {@link SecurityService#processRequest}.
     */

    private Callback<Void> securityProcessRequestCallback() {
      return buildCallback(frontendMetrics.getDatasetsSecurityProcessRequestMetrics,
          securityCheckResult -> securityService.postProcessRequest(restRequest, securityPostProcessRequestCallback()),
          uri, LOGGER, finalCallback);
    }

    /**
     * After {@link SecurityService#processRequest} finishes, call {@link SecurityService#postProcessRequest} to perform
     * request time security checks that rely on the request being fully parsed and any additional arguments set.
     * @return a {@link Callback} to be used with {@link SecurityService#processRequest}.
     */

    private Callback<Void> securityPostProcessRequestCallback() {
      return buildCallback(frontendMetrics.copyDatasetsSecurityPostProcessRequestMetrics, securityCheckResult -> {
        LOGGER.debug("Received request for copy a dataset version with arguments: {}", restRequest.getArgs());
        frontendMetrics.copyDatasetVersionRate.mark();
        copyDatasetVersions();
        restResponseChannel.setHeader(RestUtils.Headers.DATE, new GregorianCalendar().getTime());
        restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, 0);
        finalCallback.onCompletion(null, null);
      }, uri, LOGGER, finalCallback);
    }

    /**
     * List all valid dataset versions under the specific dataset.
     * @return the page of the dataset version.
     * @throws RestServiceException
     */

    private void copyDatasetVersions() throws RestServiceException {
      long startListDatasetVersionTime = System.currentTimeMillis();
      String accountName = null;
      String containerName = null;
      String sourceDatasetVersion = null;
      String targetDatasetVersion = null;
      String datasetName = null;
      try {
        DatasetVersionPath datasetVersionPath =
            DatasetVersionPath.parse(RestUtils.getRequestPath(restRequest), restRequest.getArgs());
        accountName = datasetVersionPath.getAccountName();
        containerName = datasetVersionPath.getContainerName();
        datasetName = datasetVersionPath.getDatasetName();
        sourceDatasetVersion = datasetVersionPath.getVersion();
        targetDatasetVersion = datasetVersionPath.getTargetVersion();
        accountService.renameDatasetVersion(accountName, containerName, datasetName, sourceDatasetVersion,
            targetDatasetVersion);
        frontendMetrics.copyDatasetVersionProcessingTimeInMs.update(
            System.currentTimeMillis() - startListDatasetVersionTime);
      } catch (AccountServiceException ex) {
        LOGGER.error("Dataset version rename failed for accountName " + accountName + " containerName " + containerName
            + " datasetName " + datasetName + " sourceDatasetVersion " + sourceDatasetVersion + " targetDatasetVersion "
            + targetDatasetVersion);
        frontendMetrics.copyDatasetVersionError.inc();
        throw new RestServiceException(ex.getMessage(),
            RestServiceErrorCode.getRestServiceErrorCode(ex.getErrorCode()));
      }
    }
  }
}
