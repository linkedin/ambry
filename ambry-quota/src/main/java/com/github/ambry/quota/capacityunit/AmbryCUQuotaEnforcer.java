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
import com.github.ambry.quota.QuotaEnforcer;
import com.github.ambry.quota.QuotaException;
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
 * Implementation of {@link QuotaEnforcer} for CU quota of Ambry's {@link QuotaResource}s.
 * Note that this implementation is not thread safe or atomic. The caller should take care of thread safety guarantees
 * where needed.
 */
public class AmbryCUQuotaEnforcer implements QuotaEnforcer {
  private final static List<QuotaName> SUPPORTED_QUOTA_NAMES =
      Collections.unmodifiableList(Arrays.asList(QuotaName.READ_CAPACITY_UNIT, QuotaName.WRITE_CAPACITY_UNIT));
  private final static long NO_THROTTLE_RETRY_AFTER_MS = -1;
  private final QuotaSource quotaSource;
  private final float maxFrontendCuUsageToAllowExceed;
  private final long throttleRetryAfterMs;

  /**
   * Constructor for {@link AmbryCapacityUnitQuotaEnforcer}.
   * @param quotaSource {@link QuotaSource} where the quota limit and usage will be saved and retrieved from.
   */
  public AmbryCUQuotaEnforcer(QuotaSource quotaSource, QuotaConfig quotaConfig) {
    this.quotaSource = quotaSource;
    this.throttleRetryAfterMs = quotaConfig.cuQuotaAggregationWindowInSecs;
    this.maxFrontendCuUsageToAllowExceed = quotaConfig.maxFrontendCuUsageToAllowExceed;
  }

  @Override
  public void init() {
  }

  @Override
  public QuotaRecommendation charge(RestRequest restRequest, Map<QuotaName, Double> requestCostMap) throws QuotaException {
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
    float usage = doAndHandleException(() -> {
      final QuotaResource quotaResource = QuotaResource.fromRestRequest(restRequest);
      return quotaSource.getUsage(quotaResource, quotaName);
    }, String.format("Could not recommend for request %s due to", restRequest));
    return buildQuotaRecommendation(usage, quotaName);
  }

  @Override
  public boolean isQuotaExceedAllowed(RestRequest restRequest) throws QuotaException {
    return doAndHandleException(() -> quotaSource.getSystemResourceUsage(QuotaUtils.getCUQuotaName(restRequest))
            < maxFrontendCuUsageToAllowExceed,
        String.format("Could not check quota exceed allowed for request %s due to", restRequest));
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
   * @return QuotaRecommendation object.
   */
  private QuotaRecommendation buildQuotaRecommendation(float usage, QuotaName quotaName) {
    boolean shouldThrottle = (usage >= 100);
    return new QuotaRecommendation(shouldThrottle, usage, quotaName,
        shouldThrottle ? throttleRetryAfterMs : NO_THROTTLE_RETRY_AFTER_MS);
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
}
