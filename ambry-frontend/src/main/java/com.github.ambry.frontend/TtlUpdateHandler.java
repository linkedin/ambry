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

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.commons.BlobId;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestRequestMetrics;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.Callback;
import com.github.ambry.router.Router;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import java.util.GregorianCalendar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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

  /**
   * Constructs a handler for handling requests for updating TTLs of blobs
   * @param router the {@link Router} to use.
   * @param securityService the {@link SecurityService} to use.
   * @param idConverter the {@link IdConverter} to use.
   * @param accountAndContainerInjector helper to resolve account and container for a given request.
   * @param metrics {@link FrontendMetrics} instance where metrics should be recorded.
   * @param clusterMap the {@link ClusterMap} in use.
   */
  TtlUpdateHandler(Router router, SecurityService securityService, IdConverter idConverter,
      AccountAndContainerInjector accountAndContainerInjector, FrontendMetrics metrics, ClusterMap clusterMap) {
    this.router = router;
    this.securityService = securityService;
    this.idConverter = idConverter;
    this.accountAndContainerInjector = accountAndContainerInjector;
    this.metrics = metrics;
    this.clusterMap = clusterMap;
  }

  /**
   * Handles a request for updating the TTL of a blob
   * @param restRequest the {@link RestRequest} that contains the request parameters.
   * @param restResponseChannel the {@link RestResponseChannel} where headers should be set.
   * @param callback the {@link Callback} to invoke when the response is ready (or if there is an exception).
   */
  void handle(RestRequest restRequest, RestResponseChannel restResponseChannel, Callback<Void> callback) {
    RestRequestMetrics requestMetrics =
        restRequest.getSSLSession() != null ? metrics.updateBlobTtlSSLMetrics : metrics.updateBlobTtlMetrics;
    restRequest.getMetricsTracker().injectMetrics(requestMetrics);
    restRequest.setArg(RestUtils.InternalKeys.KEEP_ALIVE_ON_ERROR_HINT, true);
    securityService.processRequest(restRequest,
        new SecurityProcessRequestCallback(restRequest, restResponseChannel, callback));
  }

  /**
   * Callback for {@link SecurityService#processRequest(RestRequest, Callback)} that subsequently issues a call to
   * the {@link IdConverter}
   */
  private class SecurityProcessRequestCallback implements Callback<Void> {
    private final RestRequest restRequest;
    private final RestResponseChannel restResponseChannel;
    private final Callback<Void> callback;
    private final long operationStartTimeMs = SystemTime.getInstance().milliseconds();

    /**
     * @param restRequest the {@link RestRequest} received
     * @param restResponseChannel the {@link RestResponseChannel} to return the response over
     * @param callback the {@link Callback} to invoke when the response is ready (or if there is an exception).
     */
    SecurityProcessRequestCallback(RestRequest restRequest, RestResponseChannel restResponseChannel,
        Callback<Void> callback) {
      this.restRequest = restRequest;
      this.restResponseChannel = restResponseChannel;
      this.callback = callback;
    }

    @Override
    public void onCompletion(Void result, Exception exception) {
      long processingStartTimeMs = SystemTime.getInstance().milliseconds();
      metrics.updateBlobTtlSecurityRequestTimeInMs.update(processingStartTimeMs - operationStartTimeMs);
      try {
        if (exception == null) {
          String blobIdStr = RestUtils.getHeader(restRequest.getArgs(), RestUtils.Headers.BLOB_ID, true);
          idConverter.convert(restRequest, blobIdStr,
              new IdConverterCallback(restRequest, restResponseChannel, callback));
        }
      } catch (Exception e) {
        exception = e;
      } finally {
        metrics.updateBlobTtlSecurityRequestCallbackProcessingTimeInMs.update(
            SystemTime.getInstance().milliseconds() - processingStartTimeMs);
        if (exception != null) {
          callback.onCompletion(null, exception);
        }
      }
    }
  }

  /**
   * Callback for calls to {@link IdConverter}
   */
  private class IdConverterCallback implements Callback<String> {
    private final RestRequest restRequest;
    private final RestResponseChannel restResponseChannel;
    private final Callback<Void> callback;
    private final long operationStartTimeMs = SystemTime.getInstance().milliseconds();

    /**
     * @param restRequest the {@link RestRequest} received
     * @param restResponseChannel the {@link RestResponseChannel} to return the response over
     * @param callback the {@link Callback} to invoke when the response is ready (or if there is an exception).
     */
    IdConverterCallback(RestRequest restRequest, RestResponseChannel restResponseChannel, Callback<Void> callback) {
      this.restRequest = restRequest;
      this.restResponseChannel = restResponseChannel;
      this.callback = callback;
    }

    @Override
    public void onCompletion(String result, Exception exception) {
      long processingStartTimeMs = SystemTime.getInstance().milliseconds();
      metrics.idConverterProcessingTimeInMs.update(processingStartTimeMs - operationStartTimeMs);
      try {
        if (exception == null) {
          BlobId blobId = FrontendUtils.getBlobIdFromString(result, clusterMap);
          accountAndContainerInjector.injectTargetAccountAndContainerFromBlobId(blobId, restRequest);
          securityService.postProcessRequest(restRequest,
              new SecurityPostProcessRequestCallback(restRequest, restResponseChannel, blobId, callback));
        }
      } catch (Exception e) {
        exception = e;
      } finally {
        metrics.updateBlobTtlIdConverterCallbackProcessingTimeInMs.update(
            SystemTime.getInstance().milliseconds() - processingStartTimeMs);
        if (exception != null) {
          callback.onCompletion(null, exception);
        }
      }
    }
  }

  /**
   * Callback for {@link SecurityService#postProcessRequest(RestRequest, Callback)} that forwards the TTL update
   * request to the {@link Router} on success.
   */
  private class SecurityPostProcessRequestCallback implements Callback<Void> {
    private final RestRequest restRequest;
    private final RestResponseChannel restResponseChannel;
    private final BlobId blobId;
    private final Callback<Void> callback;
    private final long operationStartTimeMs = SystemTime.getInstance().milliseconds();

    /**
     * @param restRequest the {@link RestRequest} received
     * @param restResponseChannel the {@link RestResponseChannel} to return the response over
     * @param blobId the {@link BlobId} to operate on
     * @param callback the {@link Callback} to invoke when the response is ready (or if there is an exception).
     */
    SecurityPostProcessRequestCallback(RestRequest restRequest, RestResponseChannel restResponseChannel, BlobId blobId,
        Callback<Void> callback) {
      this.restRequest = restRequest;
      this.restResponseChannel = restResponseChannel;
      this.blobId = blobId;
      this.callback = callback;
    }

    @Override
    public void onCompletion(Void result, Exception exception) {
      long processingStartTimeMs = SystemTime.getInstance().milliseconds();
      metrics.updateBlobTtlSecurityPostProcessRequestTimeInMs.update(processingStartTimeMs - operationStartTimeMs);
      try {
        if (exception == null) {
          String serviceId = RestUtils.getHeader(restRequest.getArgs(), RestUtils.Headers.SERVICE_ID, true);
          router.updateBlobTtl(blobId.getID(), serviceId, Utils.Infinite_Time,
              new RouterCallback(restRequest, restResponseChannel, callback));
        }
      } catch (Exception e) {
        exception = e;
      } finally {
        metrics.updateBlobTtlSecurityPostProcessRequestCallbackProcessingTimeInMs.update(
            SystemTime.getInstance().milliseconds() - processingStartTimeMs);
        if (exception != null) {
          callback.onCompletion(null, exception);
        }
      }
    }
  }

  /**
   * Callback for update blob TTL in the {@link Router}
   */
  private class RouterCallback implements Callback<Void> {
    private final RestRequest restRequest;
    private final RestResponseChannel restResponseChannel;
    private final Callback<Void> callback;
    private final long operationStartTimeMs = SystemTime.getInstance().milliseconds();

    /**
     * @param restRequest the {@link RestRequest} received
     * @param restResponseChannel the {@link RestResponseChannel} to return the response over
     * @param callback the {@link Callback} to invoke when the response is ready (or if there is an exception).
     */
    RouterCallback(RestRequest restRequest, RestResponseChannel restResponseChannel, Callback<Void> callback) {
      this.restRequest = restRequest;
      this.restResponseChannel = restResponseChannel;
      this.callback = callback;
    }

    @Override
    public void onCompletion(Void result, Exception exception) {
      long processingStartTimeMs = SystemTime.getInstance().milliseconds();
      metrics.updateBlobTtlRouterTimeInMs.update(processingStartTimeMs - operationStartTimeMs);
      try {
        if (exception == null) {
          LOGGER.debug("Updated TTL of {}",
              RestUtils.getHeader(restRequest.getArgs(), RestUtils.Headers.BLOB_ID, true));
          restResponseChannel.setHeader(RestUtils.Headers.DATE, new GregorianCalendar().getTime());
          restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, 0);
          final long processResponseStartTimeMs = SystemTime.getInstance().milliseconds();
          securityService.processResponse(restRequest, restResponseChannel, null,
              (processResponseResult, processResponseException) -> {
                metrics.updateBlobTtlSecurityResponseTimeInMs.update(
                    SystemTime.getInstance().milliseconds() - processResponseStartTimeMs);
                callback.onCompletion(null, processResponseException);
              });
        }
      } catch (Exception e) {
        exception = e;
      } finally {
        metrics.updateBlobTtlRouterCallbackTimeInMs.update(
            SystemTime.getInstance().milliseconds() - processingStartTimeMs);
        if (exception != null) {
          callback.onCompletion(null, exception);
        }
      }
    }
  }
}
