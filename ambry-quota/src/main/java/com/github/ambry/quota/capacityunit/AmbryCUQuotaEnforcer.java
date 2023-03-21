/*
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
package com.github.ambry.quota.capacityunit;

import com.github.ambry.config.QuotaConfig;
import com.github.ambry.quota.QuotaAction;
import com.github.ambry.quota.QuotaEnforcer;
import com.github.ambry.quota.QuotaException;
import com.github.ambry.quota.QuotaMetrics;
import com.github.ambry.quota.QuotaName;
import com.github.ambry.quota.QuotaRecommendation;
import com.github.ambry.quota.QuotaResource;
import com.github.ambry.quota.QuotaSource;
import com.github.ambry.quota.QuotaUtils;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestUtils;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;


/**
 * Implementation of {@link QuotaEnforcer} for CU quota of Ambry's {@link QuotaResource}s. It recommends out of quota
 * requests to be delayed to bring them under quota.
 *
 * This implementation is not thread safe or atomic. The caller should take care of thread safety guarantees
 * where needed.
 */
public class AmbryCUQuotaEnforcer implements QuotaEnforcer {
  protected final static int MAX_USAGE_PERCENTAGE_ALLOWED = 100;
  private final static List<QuotaName> SUPPORTED_QUOTA_NAMES =
      Collections.unmodifiableList(Arrays.asList(QuotaName.READ_CAPACITY_UNIT, QuotaName.WRITE_CAPACITY_UNIT));
  protected final long throttleRetryAfterMs;
  private final QuotaSource quotaSource;
  private final boolean requestThrottlingEnabled;
  private final float maxFrontendCuUsageToAllowExceed;
  private final QuotaMetrics quotaMetrics;

  /**
   * Constructor for {@link AmbryCUQuotaEnforcer}.
   * @param quotaSource {@link QuotaSource} where the quota limit and usage will be saved and retrieved from.
   * @param quotaConfig {@link QuotaConfig} object.
   * @param quotaMetrics {@link QuotaMetrics} object.
   */
  public AmbryCUQuotaEnforcer(QuotaSource quotaSource, QuotaConfig quotaConfig, QuotaMetrics quotaMetrics) {
    this.quotaSource = quotaSource;
    this.throttleRetryAfterMs = quotaConfig.cuQuotaAggregationWindowInSecs;
    this.maxFrontendCuUsageToAllowExceed = quotaConfig.maxFrontendCuUsageToAllowExceed;
    this.requestThrottlingEnabled = quotaConfig.requestThrottlingEnabled;
    this.quotaMetrics = quotaMetrics;
  }

  @Override
  public void init() throws QuotaException {
    quotaSource.init();
  }

  @Override
  public QuotaRecommendation charge(RestRequest restRequest, Map<QuotaName, Double> requestCostMap)
      throws QuotaException {
    final QuotaName quotaName = QuotaUtils.getCUQuotaName(restRequest);
    if (requestCostMap.isEmpty() || !requestCostMap.containsKey(quotaName)) {
      String errorMessage = String.format("No %s cost provided for request %s. Nothing to charge", quotaName.name(),
          RestUtils.convertToStr(restRequest));
      throw new QuotaException(errorMessage, true);
    }
    final QuotaResource quotaResource = QuotaResource.fromRestRequest(restRequest);

    doAndHandleException(() -> {
      quotaSource.chargeUsage(quotaResource, quotaName, requestCostMap.get(quotaName));
      return null;
    }, String.format("Could not charge for request %s due to", restRequest));
    return recommend(restRequest);
  }

  @Override
  public QuotaRecommendation recommend(RestRequest restRequest) throws QuotaException {
    final QuotaName quotaName = QuotaUtils.getCUQuotaName(restRequest);
    final QuotaResource quotaResource = QuotaResource.fromRestRequest(restRequest);
    final float usage = doAndHandleException(() -> quotaSource.getUsage(quotaResource, quotaName),
        String.format("Could not recommend for request %s with resourceid %s due to", restRequest.getPath(),
            quotaResource.getResourceId()));
    return buildQuotaRecommendation(usage, quotaName, quotaResource.getResourceId());
  }

  @Override
  public boolean isQuotaExceedAllowed(RestRequest restRequest) throws QuotaException {
    QuotaResource quotaResource = QuotaResource.fromRestRequest(restRequest);
    return doAndHandleException(() -> {
      if (quotaSource.getSystemResourceUsage(QuotaUtils.getCUQuotaName(restRequest))
          >= maxFrontendCuUsageToAllowExceed) {
        quotaMetrics.highSystemResourceUsageCount.inc();
        if (quotaMetrics.perQuotaResourceDelayedRequestMap.containsKey(quotaResource.getResourceId())) {
          quotaMetrics.perQuotaResourceDelayedRequestMap.get(quotaResource.getResourceId()).inc();
        }
        return false;
      }
      return true;
    }, String.format("Could not check quota exceed allowed for request %s due to", restRequest));
  }

  @Override
  public List<QuotaName> supportedQuotaNames() {
    return SUPPORTED_QUOTA_NAMES;
  }

  @Override
  public QuotaSource getQuotaSource() {
    return quotaSource;
  }

  @Override
  public void shutdown() {

  }

  /**
   * Build the {@link QuotaRecommendation} object from the specified usage and {@link QuotaName}.
   * @param usage percentage usage.
   * @param quotaName {@link QuotaName} object.
   * @param resourceId resource id of the resource for which check is being done.
   * @return QuotaRecommendation object.
   */
  protected QuotaRecommendation buildQuotaRecommendation(float usage, QuotaName quotaName, String resourceId) {
    QuotaAction quotaAction = QuotaAction.ALLOW;
    if (usage >= MAX_USAGE_PERCENTAGE_ALLOWED) {
      if (quotaMetrics.perQuotaResourceOutOfQuotaMap.containsKey(resourceId)) {
        quotaMetrics.perQuotaResourceOutOfQuotaMap.get(resourceId).inc();
      }
      if (requestThrottlingEnabled) {
        quotaAction = QuotaAction.DELAY;
      }
      // Check if this request would have been throttled and log metrics.
      logMetricIfThrottleCandidate(quotaName, resourceId);
    }
    return new QuotaRecommendation(quotaAction, usage, quotaName,
        (quotaAction == QuotaAction.DELAY) ? throttleRetryAfterMs : QuotaRecommendation.NO_THROTTLE_RETRY_AFTER_MS);
  }

  /**
   * Executes the action. If the action throws an {@link Exception} this method converts it into {@link QuotaException}
   * using the specified parameters for the error message.
   * @param action the {@link Callable} to call.
   * @param errorMessagePrefix exception error message to use as prefix.
   * @param <T> the result type for {@link Callable}.
   * @return result of the action.
   * @throws QuotaException in case action throws an exception.
   */
  private <T> T doAndHandleException(Callable<T> action, String errorMessagePrefix) throws QuotaException {
    try {
      return action.call();
    } catch (Exception ex) {
      if (ex instanceof QuotaException) {
        throw new QuotaException(String.format("%s %s", errorMessagePrefix, ex.getMessage()),
            ((QuotaException) ex).isRetryable());
      }
      throw new QuotaException(String.format("%s unexpected exception %s", errorMessagePrefix, ex.getMessage()), true);
    }
  }

  /**
   * Logs metrics if the request would have been throttled. Use these metrics to check potential throttling candidates
   * when running in {@link com.github.ambry.quota.QuotaMode#TRACKING} mode.
   * @param quotaName {@link QuotaName} for which check is being made.
   * @param resourceId resourceId for which metric will be logged.
   */
  private void logMetricIfThrottleCandidate(QuotaName quotaName, String resourceId) {
    try {
      if (quotaSource.getSystemResourceUsage(quotaName) > maxFrontendCuUsageToAllowExceed) {
        if (quotaMetrics.perQuotaResourceWouldBeThrottledMap.containsKey(resourceId)) {
          quotaMetrics.perQuotaResourceWouldBeThrottledMap.get(resourceId).inc();
        }
      }
    } catch (QuotaException quotaException) {
      // this is unlikely, but not logging here because it can potentially overwhelm logs.
    }
  }
}
