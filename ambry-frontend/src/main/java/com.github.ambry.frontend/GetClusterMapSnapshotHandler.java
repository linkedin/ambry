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
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestRequestMetrics;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.Callback;
import com.github.ambry.router.ReadableStreamChannel;
import java.nio.ByteBuffer;
import java.util.GregorianCalendar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.frontend.FrontendUtils.*;


class GetClusterMapSnapshotHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(GetClusterMapSnapshotHandler.class);

  private final SecurityService securityService;
  private final FrontendMetrics metrics;
  private final ClusterMap clusterMap;

  /**
   * Constructs a handler for handling requests for signed URLs.
   * @param securityService the {@link SecurityService} to use.
   * @param metrics {@link FrontendMetrics} instance where metrics should be recorded.
   * @param clusterMap the {@link ClusterMap} instance to use.
   */
  GetClusterMapSnapshotHandler(SecurityService securityService, FrontendMetrics metrics, ClusterMap clusterMap) {
    this.securityService = securityService;
    this.metrics = metrics;
    this.clusterMap = clusterMap;
  }

  /**
   * Asynchronously get the current snapshot of the {@link ClusterMap}
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
    }

    /**
     * Start the chain by calling {@link SecurityService#preProcessRequest}.
     */
    private void start() {
      RestRequestMetrics requestMetrics =
          metrics.getClusterMapSnapshotMetricsGroup.getRestRequestMetrics(restRequest.isSslUsed(), false);
      restRequest.getMetricsTracker().injectMetrics(requestMetrics);
      securityService.processRequest(restRequest, securityProcessRequestCallback());
    }

    /**
     * After {@link SecurityService#processRequest} finishes, call {@link SecurityService#postProcessRequest} to perform
     * request time security checks that rely on the request being fully parsed and any additional arguments set.
     * @return a {@link Callback} to be used with {@link SecurityService#processRequest}.
     */
    private Callback<Void> securityProcessRequestCallback() {
      return buildCallback(metrics.getClusterMapSnapshotSecurityProcessRequestMetrics,
          securityCheckResult -> securityService.postProcessRequest(restRequest, securityPostProcessRequestCallback()),
          restRequest.getUri(), LOGGER, finalCallback);
    }

    /**
     * After {@link SecurityService#postProcessRequest} finishes, construct the snapshot of the {@link #clusterMap}.
     * @return a {@link Callback} to be used with {@link SecurityService#postProcessRequest}.
     */
    private Callback<Void> securityPostProcessRequestCallback() {
      return buildCallback(metrics.getClusterMapSnapshotSecurityPostProcessRequestMetrics, securityCheckResult -> {
        LOGGER.trace("Getting snapshot of the cluster map");
        long startTime = System.currentTimeMillis();
        try {
          byte[] replicasResponseBytes = clusterMap.getSnapshot().toString().getBytes();
          restResponseChannel.setHeader(RestUtils.Headers.DATE, new GregorianCalendar().getTime());
          restResponseChannel.setHeader(RestUtils.Headers.CONTENT_TYPE, RestUtils.JSON_CONTENT_TYPE);
          restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, replicasResponseBytes.length);
          ReadableStreamChannel channel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(replicasResponseBytes));
          finalCallback.onCompletion(channel, null);
        } finally {
          metrics.getClusterMapSnapshotProcessingTimeInMs.update(System.currentTimeMillis() - startTime);
        }
      }, restRequest.getUri(), LOGGER, finalCallback);
    }
  }
}
