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
import com.github.ambry.account.DatasetVersionRecord;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.Callback;
import com.github.ambry.named.NamedBlobDb;
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
    private String originalBlobIdStr;

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
        originalBlobIdStr = blobIdStr;
        if (RestUtils.isDatasetVersionQueryEnabled(restRequest.getArgs())) {
          String datasetVersionPathString = RestUtils.getHeader(restRequest.getArgs(), RestUtils.Headers.BLOB_ID, true);
          DatasetVersionRecord datasetVersionRecord =
              getDatasetVersionHelper(restRequest, datasetVersionPathString, accountService, metrics);
          if (datasetVersionRecord.getRenameFrom() != null) {
            DatasetVersionPath datasetVersionPath = DatasetVersionPath.parse(blobIdStr, restRequest.getArgs());
            blobIdStr = datasetVersionRecord.getRenamedPath(datasetVersionPath.getAccountName(),
                datasetVersionPath.getContainerName());
            restRequest.setArg(RestUtils.Headers.BLOB_ID, blobIdStr);
          }
        }
        if (RequestPath.matchesOperation(blobIdStr, Operations.NAMED_BLOB)) {
          accountAndContainerInjector.injectAccountContainerForNamedBlob(restRequest, metrics.updateBlobTtlMetricsGroup);
        } else {
          String blobIdClean = RestUtils.stripSlashAndExtensionFromId(blobIdStr);
          BlobId convertedBlobId = FrontendUtils.getBlobIdFromString(blobIdClean, clusterMap);
          accountAndContainerInjector.injectTargetAccountAndContainerFromBlobId(convertedBlobId, restRequest,
              metrics.updateBlobTtlMetricsGroup);
        }
        securityService.postProcessRequest(restRequest, securityPostProcessRequestCallback());
      }, restRequest.getUri(), LOGGER, finalCallback);
    }

    /**
     * After {@link SecurityService#postProcessRequest} finishes, call {@link Router#updateBlobTtl} to update the TTL of
     * the blob in the storage layer.
     * @return a {@link Callback} to be used with {@link SecurityService#postProcessRequest}.
     */
    private Callback<Void> securityPostProcessRequestCallback() {
      return buildCallback(metrics.updateBlobTtlSecurityPostProcessRequestMetrics, result -> {
        String serviceId = RestUtils.getHeader(restRequest.getArgs(), RestUtils.Headers.SERVICE_ID, true);
        router.updateBlobTtl(restRequest, blobIdStr, serviceId, Utils.Infinite_Time, routerCallback(),
            QuotaUtils.buildQuotaChargeCallback(restRequest, quotaManager, false));
      }, restRequest.getUri(), LOGGER, finalCallback);
    }

    /**
     * After {@link Router#updateBlobTtl} finishes, call {@link SecurityService#processResponse}.
     * @return a {@link Callback} to be used with {@link Router#updateBlobTtl}.
     */
    private Callback<Void> routerCallback() {
      return buildCallback(metrics.updateBlobTtlRouterMetrics, result -> {
        //must reset the blobId to the original id (when renaming dataset) before updateTtlForDatasetVersion.
        restRequest.setArg(RestUtils.Headers.BLOB_ID, originalBlobIdStr);
        if (RequestPath.matchesOperation(blobIdStr, Operations.NAMED_BLOB) && RestUtils.isDatasetVersionQueryEnabled(
            restRequest.getArgs())) {
          metrics.updateTtlDatasetVersionRate.mark();
          updateTtlForDatasetVersion();
        }
        LOGGER.debug("Updated TTL of {}",
            RestUtils.getHeader(restRequest.getArgs(), RestUtils.Headers.BLOB_ID, true));
        restResponseChannel.setHeader(RestUtils.Headers.DATE, new GregorianCalendar().getTime());
        restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, 0);
        securityService.processResponse(restRequest, restResponseChannel, null, securityProcessResponseCallback());
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
     * Support ttl update for dataset version.
     * @throws RestServiceException
     */
    private void updateTtlForDatasetVersion() throws RestServiceException {
      long startUpdateTtlDatasetVersionTime = System.currentTimeMillis();
      String datasetVersionPathString = RestUtils.getHeader(restRequest.getArgs(), RestUtils.Headers.BLOB_ID, true);
      DatasetVersionPath datasetVersionPath = DatasetVersionPath.parse(datasetVersionPathString, restRequest.getArgs());
      String accountName = datasetVersionPath.getAccountName();
      String containerName = datasetVersionPath.getContainerName();
      String datasetName = datasetVersionPath.getDatasetName();
      String version = datasetVersionPath.getVersion();
      try {
        accountService.updateDatasetVersionTtl(accountName, containerName, datasetName, version);
        metrics.updateTtlDatasetVersionProcessingTimeInMs.update(
            System.currentTimeMillis() - startUpdateTtlDatasetVersionTime);
      } catch (AccountServiceException ex) {
        LOGGER.error(
            "Dataset version update failed for accountName: " + accountName + " containerName: " + containerName
                + " datasetName: " + datasetName + " version: " + version, ex);
        metrics.ttlUpdateDatasetVersionError.inc();
        throw new RestServiceException(ex.getMessage(),
            RestServiceErrorCode.getRestServiceErrorCode(ex.getErrorCode()));
      }
    }
  }
}
