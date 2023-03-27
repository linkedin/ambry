/**
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.account.Dataset;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.Callback;
import com.github.ambry.quota.QuotaManager;
import com.github.ambry.quota.QuotaUtils;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.ResponseStatus;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestRequestMetrics;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.Router;
import java.util.GregorianCalendar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.frontend.FrontendUtils.*;
import static com.github.ambry.frontend.Operations.*;
import static com.github.ambry.rest.RestUtils.*;
import static com.github.ambry.rest.RestUtils.InternalKeys.*;


/**
 * Handler for all delete blob requests.
 */
public class DeleteBlobHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeleteBlobHandler.class);

  private final Router router;
  private final SecurityService securityService;
  private final IdConverter idConverter;
  private final AccountAndContainerInjector accountAndContainerInjector;
  private final FrontendMetrics metrics;
  private final ClusterMap clusterMap;
  private final QuotaManager quotaManager;
  private final AccountService accountService;

  DeleteBlobHandler(Router router, SecurityService securityService, IdConverter idConverter,
      AccountAndContainerInjector accountAndContainerInjector, FrontendMetrics metrics, ClusterMap clusterMap,
      QuotaManager quotaManager, AccountService accountService) {
    this.router = router;
    this.securityService = securityService;
    this.idConverter = idConverter;
    this.accountAndContainerInjector = accountAndContainerInjector;
    this.metrics = metrics;
    this.clusterMap = clusterMap;
    this.quotaManager = quotaManager;
    this.accountService = accountService;
  }

  void handle(RestRequest restRequest, RestResponseChannel restResponseChannel, Callback<Void> callback)
      throws RestServiceException {
    RestRequestMetrics requestMetrics =
        metrics.deleteBlobMetricsGroup.getRestRequestMetrics(restRequest.isSslUsed(), false);
    RequestPath requestPath = getRequestPath(restRequest);
    // named blob requests have their account/container in the URI, so checks can be done prior to ID conversion.
    if (requestPath.matchesOperation(Operations.NAMED_BLOB)) {
      accountAndContainerInjector.injectAccountContainerAndDatasetForNamedBlob(restRequest, metrics.deleteBlobMetricsGroup);
    }
    restRequest.getMetricsTracker().injectMetrics(requestMetrics);
    new CallbackChain(restRequest, restResponseChannel, callback).start();
  }

  private class CallbackChain {
    private final RestRequest restRequest;
    private final RestResponseChannel restResponseChannel;
    private final Callback<Void> finalCallback;

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
      restRequest.setArg(RestUtils.InternalKeys.KEEP_ALIVE_ON_ERROR_HINT, true);
      securityService.processRequest(restRequest, securityProcessRequestCallback());
    }

    /**
     * After {@link SecurityService#processRequest} finishes, call {@link IdConverter#convert} to convert the incoming
     * ID if required.
     * @return a {@link Callback} to be used with {@link SecurityService#processRequest}.
     */
    private Callback<Void> securityProcessRequestCallback() {
      return buildCallback(metrics.deleteBlobSecurityProcessRequestMetrics, result -> {
        String blobIdStr = getRequestPath(restRequest).getOperationOrBlobId(false);
        idConverter.convert(restRequest, blobIdStr, idConverterCallback());
      }, restRequest.getUri(), LOGGER, finalCallback);
    }

    /**
     * After {@link IdConverter#convert} finishes, call {@link SecurityService#postProcessRequest} to perform
     * request time security checks that rely on the request being fully parsed and any additional arguments set.
     * @return a {@link Callback} to be used with {@link IdConverter#convert}.
     */
    private Callback<String> idConverterCallback() {
      return buildCallback(metrics.deleteBlobIdConversionMetrics, convertedBlobId -> {
        BlobId blobId = FrontendUtils.getBlobIdFromString(convertedBlobId, clusterMap);
        if (restRequest.getArgs().get(InternalKeys.TARGET_ACCOUNT_KEY) == null) {
          // Inject account and container when they are missing from the rest request.
          accountAndContainerInjector.injectTargetAccountAndContainerFromBlobId(blobId, restRequest,
              metrics.deleteBlobMetricsGroup);
        }
        securityService.postProcessRequest(restRequest, securityPostProcessRequestCallback(blobId));
      }, restRequest.getUri(), LOGGER, finalCallback);
    }

    /**
     * After {@link SecurityService#postProcessRequest} finishes, call {@link Router#deleteBlob} to delete
     * the blob in the storage layer.
     * @param blobId the {@link BlobId} to undelete
     * @return a {@link Callback} to be used with {@link SecurityService#postProcessRequest}.
     */
    private Callback<Void> securityPostProcessRequestCallback(BlobId blobId) {
      return buildCallback(metrics.deleteBlobSecurityPostProcessRequestMetrics, result -> {
        String serviceId = RestUtils.getHeader(restRequest.getArgs(), RestUtils.Headers.SERVICE_ID, false);
        router.deleteBlob(blobId.getID(), serviceId, routerCallback(),
            QuotaUtils.buildQuotaChargeCallback(restRequest, quotaManager, false));
      }, restRequest.getUri(), LOGGER, finalCallback);
    }

    /**
     * After {@link Router#deleteBlob} finishes, call {@link SecurityService#processResponse}.
     * @return a {@link Callback} to be used with {@link Router#deleteBlob}.
     */
    private Callback<Void> routerCallback() {
      return buildCallback(metrics.deleteBlobRouterMetrics, result -> {
        if (RestUtils.isDatasetVersionQueryEnabled(restRequest.getArgs())) {
          try {
            metrics.deleteDatasetVersionRate.mark();
            deleteDatasetVersion(restRequest);
          } catch (RestServiceException e) {
            metrics.deleteDatasetVersionError.inc();
            throw e;
          }
        }
        LOGGER.debug("Deleted {}", getRequestPath(restRequest).getOperationOrBlobId(false));
        //if delete dataset request call this handler to delete all dataset versions under the dataset, we
        //don't need to set the header and call securityService.processResponse. We should let DeleteDatasetHandler
        //take care of this.
        if (!(RequestPath.matchesOperation(restRequest.getUri(), ACCOUNTS_CONTAINERS_DATASETS)
            && restRequest.getRestMethod() == RestMethod.DELETE)) {
          restResponseChannel.setStatus(ResponseStatus.Accepted);
          restResponseChannel.setHeader(RestUtils.Headers.DATE, new GregorianCalendar().getTime());
          restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, 0);
        }
        securityService.processResponse(restRequest, restResponseChannel, null, securityProcessResponseCallback());
      }, restRequest.getUri(), LOGGER, (r, e) -> {
        // Even we failed in router operations, we already used some of the resources in router,
        // so let's record the charges for this request.
        securityService.processRequestCharges(restRequest, restResponseChannel, null);
        finalCallback.onCompletion(null, e);
      });
    }

    /**
     * After {@link SecurityService#processResponse}, call {@code finalCallback}.
     * @return a {@link Callback} to be used with {@link SecurityService#processResponse}.
     */
    private Callback<Void> securityProcessResponseCallback() {
      return buildCallback(metrics.deleteBlobSecurityProcessResponseMetrics,
          securityCheckResult -> finalCallback.onCompletion(null, null), restRequest.getUri(), LOGGER, finalCallback);
    }

    /**
     * Support delete dataset version.
     * @param restRequest restRequest {@link RestRequest} representing the request.
     * @throws RestServiceException
     */
    private void deleteDatasetVersion(RestRequest restRequest) throws RestServiceException {
      long startDeleteDatasetVersionTime = System.currentTimeMillis();
      String accountName = null;
      String containerName = null;
      String datasetName = null;
      String version = null;
      try {
        Dataset dataset = (Dataset) restRequest.getArgs().get(InternalKeys.TARGET_DATASET);
        accountName = dataset.getAccountName();
        containerName = dataset.getContainerName();
        datasetName = dataset.getDatasetName();
        version = (String) restRequest.getArgs().get(TARGET_DATASET_VERSION);
        accountService.deleteDatasetVersion(accountName, containerName, datasetName, version);
        metrics.deleteDatasetVersionProcessingTimeInMs.update(
            System.currentTimeMillis() - startDeleteDatasetVersionTime);
        // If version is null, use the latest version + 1 from DatasetVersionRecord to construct named blob path.
      } catch (AccountServiceException ex) {
        LOGGER.error(
            "Failed to get dataset version for accountName: " + accountName + " containerName: " + containerName
                + " datasetName: " + datasetName + " version: " + version);
        throw new RestServiceException(ex.getMessage(),
            RestServiceErrorCode.getRestServiceErrorCode(ex.getErrorCode()));
      }
    }
  }
}
