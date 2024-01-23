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

import com.github.ambry.account.AccountCollectionSerde;
import com.github.ambry.account.AccountService;
import com.github.ambry.account.AccountServiceException;
import com.github.ambry.account.Container;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.BlobId;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.Callback;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.protocol.GetOption;
import com.github.ambry.quota.QuotaManager;
import com.github.ambry.quota.QuotaUtils;
import com.github.ambry.rest.RequestPath;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestRequestMetrics;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.GetBlobOptions;
import com.github.ambry.router.GetBlobOptionsBuilder;
import com.github.ambry.router.GetBlobResult;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.router.Router;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.GregorianCalendar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.frontend.FrontendUtils.*;
import static com.github.ambry.rest.RestUtils.*;
import static com.github.ambry.rest.RestUtils.InternalKeys.*;


/**
 * Handler to handle all the http HEAD requests on blobs.
 */
public class HeadBlobHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(HeadBlobHandler.class);

  private final FrontendConfig frontendConfig;
  private final Router router;
  private final SecurityService securityService;
  private final IdConverter idConverter;
  private final AccountAndContainerInjector accountAndContainerInjector;
  private final FrontendMetrics metrics;
  private final ClusterMap clusterMap;
  private final QuotaManager quotaManager;
  private final AccountService accountService;

  HeadBlobHandler(FrontendConfig frontendConfig, Router router, SecurityService securityService,
      IdConverter idConverter, AccountAndContainerInjector accountAndContainerInjector, FrontendMetrics metrics,
      ClusterMap clusterMap, QuotaManager quotaManager, AccountService accountService) {
    this.frontendConfig = frontendConfig;
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
    // named blob requests have their account/container in the URI, so checks can be done prior to ID conversion.
    if (getRequestPath(restRequest).matchesOperation(Operations.NAMED_BLOB)) {
      accountAndContainerInjector.injectAccountContainerForNamedBlob(restRequest, metrics.headBlobMetricsGroup);
    }
    new CallbackChain(restRequest, restResponseChannel, callback).start();
  }

  // Add method to handle HEAD /account-name from S3
  void handleAccounts(RestRequest restRequest, RestResponseChannel restResponseChannel,
      Callback<ReadableStreamChannel> callback) throws RestServiceException {
    // named blob requests have their account/container in the URI, so checks can be done prior to ID conversion.
    if (getRequestPath(restRequest).matchesOperation(Operations.NAMED_BLOB)) {
      accountAndContainerInjector.injectAccountContainerForNamedBlob(restRequest, metrics.headBlobMetricsGroup);
    }
    new CallbackChain(restRequest, restResponseChannel, callback, false).start();
  }

  private class CallbackChain {
    private final RestRequest restRequest;
    private final RestResponseChannel restResponseChannel;
    private final Callback<Void> finalCallback;
    private final Callback<ReadableStreamChannel> finalReadableStreamChannelCallback;
    private boolean isAccountRequest = false;

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
      this.finalReadableStreamChannelCallback = null;
    }

    /**
     * @param restRequest the {@link RestRequest}.
     * @param restResponseChannel the {@link RestResponseChannel}.
     * @param finalCallback the {@link Callback} to call on completion.
     */
    private CallbackChain(RestRequest restRequest, RestResponseChannel restResponseChannel,
        Callback<ReadableStreamChannel> finalCallback, boolean unused) {
      this.restRequest = restRequest;
      this.restResponseChannel = restResponseChannel;
      this.finalReadableStreamChannelCallback = finalCallback;
      this.finalCallback = null;
      this.isAccountRequest = true;
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
      return buildCallback(metrics.headBlobSecurityProcessRequestMetrics, result -> {
        if (!isAccountRequest) {
          String blobIdStr = getRequestPath(restRequest).getOperationOrBlobId(false);
          idConverter.convert(restRequest, blobIdStr, idConverterCallback());
        } else {
          securityService.postProcessRequest(restRequest, securityPostProcessRequestCallback());
        }
      }, restRequest.getUri(), LOGGER, finalCallback);
    }

    /**
     * After {@link IdConverter#convert} finishes, call {@link SecurityService#postProcessRequest} to perform
     * request time security checks that rely on the request being fully parsed and any additional arguments set.
     * @return a {@link Callback} to be used with {@link IdConverter#convert}.
     */
    private Callback<String> idConverterCallback() {
      return buildCallback(metrics.headBlobIdConversionMetrics, convertedBlobId -> {
        BlobId blobId = FrontendUtils.getBlobIdFromString(convertedBlobId, clusterMap);
        if (restRequest.getArgs().get(TARGET_ACCOUNT_KEY) == null) {
          // Inject account and container when they are missing from the rest request.
          accountAndContainerInjector.injectTargetAccountAndContainerFromBlobId(blobId, restRequest,
              metrics.headBlobMetricsGroup);
        }
        securityService.postProcessRequest(restRequest, securityPostProcessRequestCallback(blobId));
      }, restRequest.getUri(), LOGGER, finalCallback);
    }

    /**
     * After {@link SecurityService#postProcessRequest} finishes, call {@link Router#getBlob} to dget
     * the blob info from the storage layer.
     * @param blobId the {@link BlobId} to get info
     * @return a {@link Callback} to be used with {@link SecurityService#postProcessRequest}.
     */
    private Callback<Void> securityPostProcessRequestCallback(BlobId blobId) {
      return buildCallback(metrics.headBlobSecurityPostProcessRequestMetrics, result -> {
        GetOption getOption = getGetOption(restRequest, frontendConfig.defaultRouterGetOption);
        // inject encryption metrics if need be
        if (BlobId.isEncrypted(blobId.getID())) {
          RestRequestMetrics requestMetrics =
              metrics.headBlobMetricsGroup.getRestRequestMetrics(restRequest.isSslUsed(), true);
          restRequest.getMetricsTracker().injectMetrics(requestMetrics);
        }
        router.getBlob(blobId.getID(), new GetBlobOptionsBuilder().operationType(GetBlobOptions.OperationType.BlobInfo)
            .getOption(getOption)
            .restRequest(restRequest)
            .build(), routerCallback(), QuotaUtils.buildQuotaChargeCallback(restRequest, quotaManager, false));
      }, restRequest.getUri(), LOGGER, finalCallback);
    }

    /**
     * After {@link Router#getBlob} finishes, call {@link SecurityService#processResponse}.
     * @return a {@link Callback} to be used with {@link Router#getBlob}.
     */
    private Callback<GetBlobResult> routerCallback() {
      return buildCallback(metrics.headBlobRouterMetrics, result -> {
        LOGGER.debug("Head {}", getRequestPath(restRequest).getOperationOrBlobId(false));
        accountAndContainerInjector.ensureAccountAndContainerInjected(restRequest,
            result.getBlobInfo().getBlobProperties(), metrics.headBlobMetricsGroup);
        securityService.processResponse(restRequest, restResponseChannel, result.getBlobInfo(),
            securityProcessResponseCallback());
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
      return buildCallback(metrics.headBlobSecurityProcessResponseMetrics,
          securityCheckResult -> finalCallback.onCompletion(null, null), restRequest.getUri(), LOGGER, finalCallback);
    }

    /**
     * After {@link SecurityService#postProcessRequest} finishes, call the final callback with the response body to
     * sen
     * @return a {@link Callback} to be used with {@link SecurityService#postProcessRequest}.
     */
    private Callback<Void> securityPostProcessRequestCallback() {
      return buildCallback(metrics.getAccountsSecurityPostProcessRequestMetrics, securityCheckResult -> {
        byte[] serialized;
        RequestPath requestPath = (RequestPath) restRequest.getArgs().get(REQUEST_PATH);
        NamedBlobPath namedBlobPath = NamedBlobPath.parse(requestPath, restRequest.getArgs());
        restRequest.setArg(Headers.TARGET_ACCOUNT_NAME, namedBlobPath.getAccountName());
        restRequest.setArg(Headers.TARGET_CONTAINER_NAME, namedBlobPath.getContainerName());
        LOGGER.debug("Received head request for named blob account and container: {}",
            restRequest.getArgs().get(REQUEST_PATH));
        Container container = getContainer();
        serialized = AccountCollectionSerde.serializeContainersInJson(Collections.singletonList(container));
        restResponseChannel.setHeader(RestUtils.Headers.TARGET_ACCOUNT_ID, container.getParentAccountId());
        ReadableStreamChannel channel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(serialized));
        restResponseChannel.setHeader(RestUtils.Headers.DATE, new GregorianCalendar().getTime());
        restResponseChannel.setHeader(RestUtils.Headers.CONTENT_TYPE, RestUtils.JSON_CONTENT_TYPE);
        restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, channel.getSize());
        finalReadableStreamChannelCallback.onCompletion(channel, null);
      }, restRequest.getUri(), LOGGER, finalReadableStreamChannelCallback);
    }

    /**
     * @return requested container.
     * @throws RestServiceException
     */
    private Container getContainer() throws RestServiceException {
      String accountName = RestUtils.getHeader(restRequest.getArgs(), RestUtils.Headers.TARGET_ACCOUNT_NAME, true);
      String containerName = RestUtils.getHeader(restRequest.getArgs(), RestUtils.Headers.TARGET_CONTAINER_NAME, true);
      Container container;
      try {
        container = accountService.getContainerByName(accountName, containerName);
      } catch (AccountServiceException e) {
        throw new RestServiceException("Failed to get container " + containerName + " from account " + accountName,
            RestServiceErrorCode.getRestServiceErrorCode(e.getErrorCode()));
      }
      if (container == null) {
        throw new RestServiceException("Container " + containerName + " in account " + accountName + " is not found.",
            RestServiceErrorCode.NotFound);
      }
      return container;
    }
  }
}
