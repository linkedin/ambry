/**
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
 */
package com.github.ambry.frontend;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.accountstats.AccountStatsStore;
import com.github.ambry.commons.ByteBufferReadableStreamChannel;
import com.github.ambry.commons.Callback;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestRequestMetrics;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.server.StatsReportType;
import java.nio.ByteBuffer;
import java.util.GregorianCalendar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.frontend.FrontendUtils.*;


class GetStatsReportHandler {
  private static final Logger logger = LoggerFactory.getLogger(GetStatsReportHandler.class);

  private final ObjectMapper mapper = new ObjectMapper();
  private final SecurityService securityService;
  private final FrontendMetrics metrics;
  private final AccountStatsStore accountStatsStore;

  /**
   * Constructs a handler for handling requests for getting stats reports.
   * @param securityService the {@link SecurityService} to use.
   * @param metrics the {@link FrontendMetrics} to use.
   * @param accountStatsStore the {@link AccountStatsStore} to use.
   */
  GetStatsReportHandler(SecurityService securityService, FrontendMetrics metrics, AccountStatsStore accountStatsStore) {
    this.securityService = securityService;
    this.metrics = metrics;
    this.accountStatsStore = accountStatsStore;
  }

  /**
   * Asynchronously get the stats reports.
   * @param restRequest the {@link RestRequest} that contains the request parameters.
   * @param restResponseChannel the {@link RestResponseChannel} where headers should be set.
   * @param callback the {@link Callback} to invoke when the response is ready or if there is a exception.
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
     * Constructor to instantiate a {@link CallbackChain}.
     * @param restRequest the {@link RestRequest}.
     * @param restResponseChannel The {@link RestResponseChannel}.
     * @param finalCallback The {@link Callback} to call on completion.
     */
    private CallbackChain(RestRequest restRequest, RestResponseChannel restResponseChannel,
        Callback<ReadableStreamChannel> finalCallback) {
      this.restRequest = restRequest;
      this.restResponseChannel = restResponseChannel;
      this.finalCallback = finalCallback;
    }

    /**
     * Start the chain by calling {@link SecurityService#processRequest}.
     */
    private void start() {
      RestRequestMetrics requestMetrics =
          metrics.getStatsReportMetricsGroup.getRestRequestMetrics(restRequest.isSslUsed(), false);
      restRequest.getMetricsTracker().injectMetrics(requestMetrics);
      // Start the callback chain by performing request security processing.
      securityService.processRequest(restRequest, securityProcessRequestCallback());
    }

    /**
     * After {@link SecurityService#processRequest} finishes, call {@link SecurityService#postProcessRequest} to perform
     * request time security checks that rely on the request being fully parsed and any additional arguments set.
     * @return a {@link Callback} to be used with {@link SecurityService#processRequest}.
     */
    private Callback<Void> securityProcessRequestCallback() {
      return buildCallback(metrics.getStatsReportSecurityProcessRequestMetrics,
          securityCheckResult -> securityService.postProcessRequest(restRequest, securityPostProcessRequestCallback()),
          restRequest.getUri(), logger, finalCallback);
    }

    /**
     * After {@link SecurityService#postProcessRequest} finishes, fetch stats report from {@link AccountStatsStore} and construct
     * a json object to send back to {@link RestResponseChannel}.
     * @return a {@link Callback} to be used with {@link SecurityService#postProcessRequest}.
     */
    private Callback<Void> securityPostProcessRequestCallback() {
      return buildCallback(metrics.getStatsReportSecurityPostProcessRequestMetrics, securityCheckResult -> {
        String clusterName = RestUtils.getHeader(restRequest.getArgs(), RestUtils.Headers.CLUSTER_NAME, true);
        String reportType = RestUtils.getHeader(restRequest.getArgs(), RestUtils.Headers.GET_STATS_REPORT_TYPE, true);
        StatsReportType statsReportType;
        try {
          statsReportType = StatsReportType.valueOf(reportType);
        } catch (Exception e) {
          throw new RestServiceException(reportType + " is not a valid StatsReportType",
              RestServiceErrorCode.BadRequest);
        }
        Object result;
        switch (statsReportType) {
          case ACCOUNT_REPORT:
            result = accountStatsStore.queryAggregatedAccountStorageStatsByClusterName(clusterName);
            break;
          case PARTITION_CLASS_REPORT:
            result = accountStatsStore.queryAggregatedPartitionClassStorageStatsByClusterName(clusterName);
            break;
          default:
            throw new RestServiceException("StatsReportType " + statsReportType + "not supported",
                RestServiceErrorCode.BadRequest);
        }
        if (result == null) {
          throw new RestServiceException("StatsReport not found for clusterName " + clusterName,
              RestServiceErrorCode.NotFound);
        }
        try {
          byte[] jsonBytes = mapper.writeValueAsBytes(result);
          restResponseChannel.setHeader(RestUtils.Headers.DATE, new GregorianCalendar().getTime());
          restResponseChannel.setHeader(RestUtils.Headers.CONTENT_TYPE, RestUtils.JSON_CONTENT_TYPE);
          restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, jsonBytes.length);
          ReadableStreamChannel channel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(jsonBytes));
          finalCallback.onCompletion(channel, null);
        } catch (Exception e) {
          throw new RestServiceException("Couldn't serialize snapshot", e, RestServiceErrorCode.InternalServerError);
        }
      }, restRequest.getUri(), logger, finalCallback);
    }
  }
}
