/*
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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
 *
 */

package com.github.ambry.frontend;

import com.github.ambry.commons.Callback;
import com.github.ambry.commons.CallbackUtils;
import com.github.ambry.named.NamedBlobDb;
import com.github.ambry.named.NamedBlobRecord;
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


/**
 * Handles requests for listing blobs that exist in a named blob container that start with a provided prefix.
 */
public class NamedBlobListHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(NamedBlobListHandler.class);

  private final SecurityService securityService;
  private final NamedBlobDb namedBlobDb;
  private final AccountAndContainerInjector accountAndContainerInjector;
  private final FrontendMetrics frontendMetrics;

  /**
   * Constructs a handler for handling requests for listing blobs in named blob accounts.
   * @param securityService the {@link SecurityService} to use.
   * @param namedBlobDb the {@link NamedBlobDb} to use.
   * @param frontendMetrics {@link FrontendMetrics} instance where metrics should be recorded.
   */
  NamedBlobListHandler(SecurityService securityService, NamedBlobDb namedBlobDb,
      AccountAndContainerInjector accountAndContainerInjector, FrontendMetrics frontendMetrics) {
    this.securityService = securityService;
    this.namedBlobDb = namedBlobDb;
    this.accountAndContainerInjector = accountAndContainerInjector;
    this.frontendMetrics = frontendMetrics;
  }

  /**
   * Asynchronously get account metadata.
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
      try {
        RestRequestMetrics requestMetrics =
            frontendMetrics.getAccountsMetricsGroup.getRestRequestMetrics(restRequest.isSslUsed(), false);
        restRequest.getMetricsTracker().injectMetrics(requestMetrics);
        accountAndContainerInjector.injectAccountContainerAndDatasetForNamedBlob(restRequest,
            frontendMetrics.getBlobMetricsGroup);
        if (namedBlobDb == null) {
          throw new RestServiceException("Named blob support not enabled", RestServiceErrorCode.BadRequest);
        }
        // Start the callback chain by performing request security processing.
        securityService.processRequest(restRequest, securityProcessRequestCallback());
      } catch (Exception e) {
        finalCallback.onCompletion(null, e);
      }
    }

    /**
     * After {@link SecurityService#processRequest} finishes, call {@link SecurityService#postProcessRequest} to perform
     * any remaining security checks.
     * @return a {@link Callback} to be used with {@link SecurityService#processRequest}.
     */
    private Callback<Void> securityProcessRequestCallback() {
      return buildCallback(frontendMetrics.listSecurityProcessRequestMetrics,
          securityCheckResult -> securityService.postProcessRequest(restRequest, securityPostProcessRequestCallback()),
          uri, LOGGER, finalCallback);
    }

    /**
     * After {@link SecurityService#postProcessRequest} finishes, make a call to {@link NamedBlobDb#list}.
     * @return a {@link Callback} to be used with {@link SecurityService#postProcessRequest}.
     */
    private Callback<Void> securityPostProcessRequestCallback() {
      return buildCallback(frontendMetrics.listSecurityPostProcessRequestMetrics, securityCheckResult -> {
        NamedBlobPath namedBlobPath = NamedBlobPath.parse(RestUtils.getRequestPath(restRequest), restRequest.getArgs());
        CallbackUtils.callCallbackAfter(
            namedBlobDb.list(namedBlobPath.getAccountName(), namedBlobPath.getContainerName(),
                namedBlobPath.getBlobNamePrefix(), namedBlobPath.getPageToken()), listBlobsCallback());
      }, uri, LOGGER, finalCallback);
    }

    /**
     * After {@link NamedBlobDb#list} finishes, serialize the result to JSON and send the response.
     * @return a {@link Callback} to be used with {@link NamedBlobDb#list}.
     */
    private Callback<Page<NamedBlobRecord>> listBlobsCallback() {
      return buildCallback(frontendMetrics.listDbLookupMetrics, page -> {
        ReadableStreamChannel channel =
            serializeJsonToChannel(page.toJson(record -> new NamedBlobListEntry(record).toJson()));
        restResponseChannel.setHeader(RestUtils.Headers.DATE, new GregorianCalendar().getTime());
        restResponseChannel.setHeader(RestUtils.Headers.CONTENT_TYPE, RestUtils.JSON_CONTENT_TYPE);
        restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, channel.getSize());
        finalCallback.onCompletion(channel, null);
      }, uri, LOGGER, finalCallback);
    }
  }
}
