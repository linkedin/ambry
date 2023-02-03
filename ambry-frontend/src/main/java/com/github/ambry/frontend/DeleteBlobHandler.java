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

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.Callback;
import com.github.ambry.quota.QuotaManager;
import com.github.ambry.quota.QuotaUtils;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.ResponseStatus;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestRequestMetrics;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.Router;
import java.util.GregorianCalendar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.frontend.FrontendUtils.*;
import static com.github.ambry.rest.RestUtils.*;


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

  DeleteBlobHandler(Router router, SecurityService securityService, IdConverter idConverter,
      AccountAndContainerInjector accountAndContainerInjector, FrontendMetrics metrics, ClusterMap clusterMap,
      QuotaManager quotaManager) {
    this.router = router;
    this.securityService = securityService;
    this.idConverter = idConverter;
    this.accountAndContainerInjector = accountAndContainerInjector;
    this.metrics = metrics;
    this.clusterMap = clusterMap;
    this.quotaManager = quotaManager;
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
        LOGGER.debug("Deleted {}", getRequestPath(restRequest).getOperationOrBlobId(false));
        restResponseChannel.setStatus(ResponseStatus.Accepted);
        restResponseChannel.setHeader(RestUtils.Headers.DATE, new GregorianCalendar().getTime());
        restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, 0);
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
  }
}
