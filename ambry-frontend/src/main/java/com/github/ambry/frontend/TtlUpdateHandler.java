/*
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.Callback;
import com.github.ambry.named.NamedBlobDb;
import com.github.ambry.named.NamedBlobRecord;
import com.github.ambry.quota.QuotaManager;
import com.github.ambry.quota.QuotaUtils;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestRequestMetrics;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.Router;
import com.github.ambry.utils.Utils;
import java.util.GregorianCalendar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.frontend.FrontendUtils.*;
import static com.github.ambry.rest.RestUtils.InternalKeys.*;


/**
 * Handler for all TTL update requests
 */
class TtlUpdateHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(TtlUpdateHandler.class);

  private final Router router;
  private final SecurityService securityService;
  private final IdConverter idConverter;
  private final AccountAndContainerInjector accountAndContainerInjector;
  private final FrontendMetrics metrics;
  private final ClusterMap clusterMap;
  private final QuotaManager quotaManager;
  private final NamedBlobDb namedBlobDb;
  private final AccountService accountService;

  /**
   * Constructs a handler for handling requests for updating TTLs of blobs
   * @param router the {@link Router} to use.
   * @param securityService the {@link SecurityService} to use.
   * @param idConverter the {@link IdConverter} to use.
   * @param accountAndContainerInjector helper to resolve account and container for a given request.
   * @param metrics {@link FrontendMetrics} instance where metrics should be recorded.
   * @param clusterMap the {@link ClusterMap} in use.
   * @param quotaManager {@link QuotaManager} object.
   * @param namedBlobDb the {@link NamedBlobDb} object.
   * @param accountService the {@link AccountService} object.
   */
  TtlUpdateHandler(Router router, SecurityService securityService, IdConverter idConverter,
      AccountAndContainerInjector accountAndContainerInjector, FrontendMetrics metrics, ClusterMap clusterMap,
      QuotaManager quotaManager, NamedBlobDb namedBlobDb, AccountService accountService) {
    this.router = router;
    this.securityService = securityService;
    this.idConverter = idConverter;
    this.accountAndContainerInjector = accountAndContainerInjector;
    this.metrics = metrics;
    this.clusterMap = clusterMap;
    this.quotaManager = quotaManager;
    this.namedBlobDb = namedBlobDb;
    this.accountService = accountService;
  }

  /**
   * Handles a request for updating the TTL of a blob
   * @param restRequest the {@link RestRequest} that contains the request parameters.
   * @param restResponseChannel the {@link RestResponseChannel} where headers should be set.
   * @param callback the {@link Callback} to invoke when the response is ready (or if there is an exception).
   */
  void handle(RestRequest restRequest, RestResponseChannel restResponseChannel, Callback<Void> callback) {
    new CallbackChain(restRequest, restResponseChannel, callback).start();
  }

  /**
   * Represents the chain of actions to take. Keeps request context that is relevant to all callback stages.
   */
  private class CallbackChain {
    private final RestRequest restRequest;
    private final RestResponseChannel restResponseChannel;
    private final Callback<Void> finalCallback;
    private String blobIdStr;

    /**
     * @param restRequest the {@link RestRequest}.
     * @param restResponseChannel the {@link RestResponseChannel}.
     * @param finalCallback the {@link Callback} to call on completion.
     */
    private CallbackChain(RestRequest restRequest, RestResponseChannel restResponseChannel,
        Callback<Void> finalCallback) {
      this.restRequest = restRequest;
      this.restResponseChannel = restResponseChannel;
      this.finalCallback = finalCallback;
    }

    /**
     * Start the chain by calling {@link SecurityService#processRequest}.
     */
    private void start() {
      RestRequestMetrics requestMetrics =
          metrics.updateBlobTtlMetricsGroup.getRestRequestMetrics(restRequest.isSslUsed(), false);
      restRequest.getMetricsTracker().injectMetrics(requestMetrics);
      restRequest.setArg(RestUtils.InternalKeys.KEEP_ALIVE_ON_ERROR_HINT, true);
      securityService.processRequest(restRequest, securityProcessRequestCallback());
    }

    /**
     * After {@link SecurityService#processRequest} finishes, call {@link IdConverter#convert} to convert the incoming
     * ID if required.
     * @return a {@link Callback} to be used with {@link SecurityService#processRequest}.
     */
    private Callback<Void> securityProcessRequestCallback() {
      return buildCallback(metrics.updateBlobTtlSecurityProcessRequestMetrics, result -> {
        blobIdStr = RestUtils.getHeader(restRequest.getArgs(), RestUtils.Headers.BLOB_ID, true);
        idConverter.convert(restRequest, blobIdStr, idConverterCallback());
      }, restRequest.getUri(), LOGGER, finalCallback);
    }

    /**
     * After {@link IdConverter#convert} finishes, call {@link SecurityService#postProcessRequest} to perform
     * request time security checks that rely on the request being fully parsed and any additional arguments set.
     * @return a {@link Callback} to be used with {@link IdConverter#convert}.
     */
    private Callback<String> idConverterCallback() {
      return buildCallback(metrics.updateBlobTtlIdConversionMetrics, convertedBlobIdStr -> {
        BlobId convertedBlobId = FrontendUtils.getBlobIdFromString(convertedBlobIdStr, clusterMap);
        accountAndContainerInjector.injectTargetAccountAndContainerFromBlobId(convertedBlobId, restRequest,
            metrics.updateBlobTtlMetricsGroup);
        securityService.postProcessRequest(restRequest, securityPostProcessRequestCallback(convertedBlobId));
      }, restRequest.getUri(), LOGGER, finalCallback);
    }

    /**
     * After {@link SecurityService#postProcessRequest} finishes, call {@link Router#updateBlobTtl} to update the TTL of
     * the blob in the storage layer.
     * @param convertedBlobId the {@link BlobId} to update
     * @return a {@link Callback} to be used with {@link SecurityService#postProcessRequest}.
     */
    private Callback<Void> securityPostProcessRequestCallback(BlobId convertedBlobId) {
      return buildCallback(metrics.updateBlobTtlSecurityPostProcessRequestMetrics, result -> {
        String serviceId = RestUtils.getHeader(restRequest.getArgs(), RestUtils.Headers.SERVICE_ID, true);
        router.updateBlobTtl(convertedBlobId.getID(), serviceId, Utils.Infinite_Time, routerCallback(convertedBlobId),
            QuotaUtils.buildQuotaChargeCallback(restRequest, quotaManager, false));
      }, restRequest.getUri(), LOGGER, finalCallback);
    }

    /**
     * After {@link Router#updateBlobTtl} finishes, call {@link SecurityService#processResponse}.
     * @param convertedBlobId the {@link BlobId} to update
     * @return a {@link Callback} to be used with {@link Router#updateBlobTtl}.
     */
    private Callback<Void> routerCallback(BlobId convertedBlobId) {
      return buildCallback(metrics.updateBlobTtlRouterMetrics, result -> {
        if (RequestPath.matchesOperation(blobIdStr, Operations.NAMED_BLOB)) {
          // Set the named blob state to be 'READY' after the Ttl update succeed
          if (!restRequest.getArgs().containsKey(NAMED_BLOB_VERSION)) {
            throw new RestServiceException(
                "Internal key " + NAMED_BLOB_VERSION + " is required in Named Blob TTL update callback!",
                RestServiceErrorCode.InternalServerError);
          }
          long namedBlobVersion = (long) restRequest.getArgs().get(NAMED_BLOB_VERSION);
          NamedBlobPath namedBlobPath = NamedBlobPath.parse(blobIdStr, restRequest.getArgs());
          NamedBlobRecord record = new NamedBlobRecord(namedBlobPath.getAccountName(), namedBlobPath.getContainerName(),
              namedBlobPath.getBlobName(), convertedBlobId.getID(), Utils.Infinite_Time, namedBlobVersion);
          namedBlobDb.updateBlobTtlAndStateToReady(record).thenRun(() -> {
            try {
              if (RestUtils.isDatasetVersionQueryEnabled(restRequest.getArgs())) {
                updateTtlForDatasetVersion();
              }
              processResponseHelper();
            } catch (RestServiceException ex) {
              finalCallback.onCompletion(null, ex);
            }
          });
        } else {
          processResponseHelper();
        }
      }, restRequest.getUri(), LOGGER, finalCallback);
    }

    /**
     * After {@link SecurityService#processResponse}, call {@code finalCallback}.
     * @return a {@link Callback} to be used with {@link SecurityService#processResponse}.
     */
    private Callback<Void> securityProcessResponseCallback() {
      return buildCallback(metrics.updateBlobTtlSecurityProcessResponseMetrics,
          securityCheckResult -> finalCallback.onCompletion(null, null), restRequest.getUri(), LOGGER, finalCallback);
    }

    /**
     * Helper method to set the response channel and process response.
     * @throws RestServiceException
     */
    private void processResponseHelper() throws RestServiceException {
      LOGGER.debug("Updated TTL of {}",
          RestUtils.getHeader(restRequest.getArgs(), RestUtils.Headers.BLOB_ID, true));
      restResponseChannel.setHeader(RestUtils.Headers.DATE, new GregorianCalendar().getTime());
      restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, 0);
      securityService.processResponse(restRequest, restResponseChannel, null, securityProcessResponseCallback());
    }

    /**
     * Support ttl update for dataset version.
     * @throws RestServiceException
     */
    private void updateTtlForDatasetVersion() throws RestServiceException {
      String datasetVersionPathString = RestUtils.getHeader(restRequest.getArgs(), RestUtils.Headers.BLOB_ID, true);
      DatasetVersionPath datasetVersionPath = DatasetVersionPath.parse(datasetVersionPathString, restRequest.getArgs());
      String accountName = datasetVersionPath.getAccountName();
      String containerName = datasetVersionPath.getContainerName();
      String datasetName = datasetVersionPath.getDatasetName();
      String version = datasetVersionPath.getVersion();
      try {
        accountService.updateDatasetVersionTtl(accountName, containerName, datasetName, version);
      } catch (AccountServiceException ex) {
        LOGGER.error(
            "Dataset version update failed for accountName: " + accountName + " containerName: " + containerName
                + " datasetName: " + datasetName + " version: " + version);
        throw new RestServiceException(ex.getMessage(),
            RestServiceErrorCode.getRestServiceErrorCode(ex.getErrorCode()));
      }
    }
  }
}
