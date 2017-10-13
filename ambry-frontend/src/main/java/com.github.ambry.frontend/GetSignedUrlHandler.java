/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestRequestMetrics;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.Callback;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.utils.SystemTime;
import java.util.GregorianCalendar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Handler for requests for signed URLs.
 */
class GetSignedUrlHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(GetSignedUrlHandler.class);

  private final UrlSigningService urlSigningService;
  private final SecurityService securityService;
  private final FrontendMetrics metrics;

  /**
   * Constructs a handler for handling requests for signed URLs.
   * @param urlSigningService the {@link UrlSigningService} to use.
   * @param securityService the {@link SecurityService} to use.
   * @param metrics {@link FrontendMetrics} instance where metrics should be recorded.
   */
  GetSignedUrlHandler(UrlSigningService urlSigningService, SecurityService securityService, FrontendMetrics metrics) {
    this.urlSigningService = urlSigningService;
    this.securityService = securityService;
    this.metrics = metrics;
  }

  /**
   * Handles a request for getting signed URLs.
   * @param restRequest the {@link RestRequest} that contains the request parameters.
   * @param restResponseChannel the {@link RestResponseChannel} where headers should be set.
   * @param callback the {@link Callback} to invoke when the response is ready (or if there is an exception).
   */
  void handle(RestRequest restRequest, RestResponseChannel restResponseChannel,
      Callback<ReadableStreamChannel> callback) {
    RestRequestMetrics requestMetrics =
        restRequest.getSSLSession() != null ? metrics.getSignedUrlMetrics : metrics.getSignedUrlSSLMetrics;
    restRequest.getMetricsTracker().injectMetrics(requestMetrics);
    securityService.processRequest(restRequest,
        new SecurityProcessRequestCallback(restRequest, restResponseChannel, callback));
  }

  /**
   * Callback for {@link SecurityService#processRequest(RestRequest, Callback)} that subsequently calls
   * {@link SecurityService#postProcessRequest(RestRequest, Callback)}. If post processing succeeds, a signed URL will
   * be generated.
   */
  private class SecurityProcessRequestCallback implements Callback<Void> {
    private final RestRequest restRequest;
    private final RestResponseChannel restResponseChannel;
    private final Callback<ReadableStreamChannel> callback;
    private final long operationStartTimeMs;

    SecurityProcessRequestCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
        Callback<ReadableStreamChannel> callback) {
      this.restRequest = restRequest;
      this.restResponseChannel = restResponseChannel;
      this.callback = callback;
      operationStartTimeMs = SystemTime.getInstance().milliseconds();
    }

    @Override
    public void onCompletion(Void result, Exception exception) {
      long processingStartTimeMs = SystemTime.getInstance().milliseconds();
      metrics.getSignedUrlSecurityRequestTimeInMs.update(processingStartTimeMs - operationStartTimeMs);
      try {
        if (exception == null) {
          securityService.postProcessRequest(restRequest,
              new SecurityPostProcessRequestCallback(restRequest, restResponseChannel, callback));
        }
      } catch (Exception e) {
        exception = e;
      } finally {
        metrics.getSignedUrlSecurityRequestCallbackProcessingTimeInMs.update(
            SystemTime.getInstance().milliseconds() - processingStartTimeMs);
        if (exception != null) {
          callback.onCompletion(null, exception);
        }
      }
    }
  }

  /**
   * Callback for {@link SecurityService#postProcessRequest(RestRequest, Callback)} that handles generating a signed URL
   * if the security checks succeeded.
   */
  private class SecurityPostProcessRequestCallback implements Callback<Void> {

    private final RestRequest restRequest;
    private final RestResponseChannel restResponseChannel;
    private final Callback<ReadableStreamChannel> callback;
    private final long operationStartTimeMs;

    SecurityPostProcessRequestCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
        Callback<ReadableStreamChannel> callback) {
      this.restRequest = restRequest;
      this.restResponseChannel = restResponseChannel;
      this.callback = callback;
      operationStartTimeMs = SystemTime.getInstance().milliseconds();
    }

    @Override
    public void onCompletion(Void result, Exception exception) {
      long processingStartTimeMs = SystemTime.getInstance().milliseconds();
      metrics.getSignedUrlSecurityPostProcessRequestTimeInMs.update(processingStartTimeMs - operationStartTimeMs);
      try {
        if (exception == null) {
          String signedUrl = urlSigningService.getSignedUrl(restRequest);
          LOGGER.debug("Generated {} from {}", signedUrl, restRequest);
          restResponseChannel.setHeader(RestUtils.Headers.DATE, new GregorianCalendar().getTime());
          restResponseChannel.setHeader(RestUtils.Headers.SIGNED_URL, signedUrl);
          restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, 0);
        }
      } catch (Exception e) {
        exception = e;
      } finally {
        metrics.getSignedUrlProcessingTimeInMs.update(SystemTime.getInstance().milliseconds() - processingStartTimeMs);
        callback.onCompletion(null, exception);
      }
    }
  }
}
