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
 */
package com.github.ambry.quota;

import com.github.ambry.config.QuotaConfig;
import com.github.ambry.config.StorageQuotaConfig;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * An implementation of {@link QuotaRecommendationMergePolicy} that creates a {@link ThrottlingRecommendation} to throttle if any one of
 * {@link QuotaRecommendation} recommendation is to throttle, and takes the max of retry after time interval. Also
 * groups the quota usage for all the quotas.
 *
 * This Policy also respect the settings in the {@link QuotaConfig}. If the {@link QuotaConfig#requestThrottlingEnabled} is
 * false, we don't throttle on {@link QuotaName#READ_CAPACITY_UNIT} and {@link QuotaName#WRITE_CAPACITY_UNIT}. If the
 * {@link StorageQuotaConfig#shouldThrottle} is false, we don't throttle on {@link QuotaName#STORAGE_IN_GB}.
 */
public class SimpleQuotaRecommendationMergePolicy implements QuotaRecommendationMergePolicy {
  static final long DEFAULT_RETRY_AFTER_MS = ThrottlingRecommendation.NO_RETRY_AFTER_MS;
  static final QuotaUsageLevel DEFAULT_QUOTA_USAGE_LEVEL = QuotaUsageLevel.HEALTHY;

  // Percentage usage at or below this limit is healthy.
  private final int healthyUsageLevelLimit;
  // Percentage usage level above which critical warning will be generated.
  static final int CRITICAL_USAGE_LEVEL_LIMIT = 95;

  private final QuotaConfig quotaConfig;

  /**
   * Constructor to create a {@link SimpleQuotaRecommendationMergePolicy}.
   * @param quotaConfig The {@link QuotaConfig}.
   */
  public SimpleQuotaRecommendationMergePolicy(QuotaConfig quotaConfig) {
    this.quotaConfig = quotaConfig;
    this.healthyUsageLevelLimit = quotaConfig.quotaUsageWarningThresholdInPercentage;
  }

  @Override
  public ThrottlingRecommendation mergeEnforcementRecommendations(List<QuotaRecommendation> quotaRecommendations) {
    boolean shouldThrottle = false;
    Map<QuotaName, Float> quotaUsagePercentage = new HashMap<>();
    HttpResponseStatus recommendedHttpStatus = QuotaRecommendationMergePolicy.ACCEPT_HTTP_STATUS;
    long retryAfterMs = DEFAULT_RETRY_AFTER_MS;
    for (QuotaRecommendation recommendation : quotaRecommendations) {
      boolean currentQuotaShouldThrottle = recommendation.shouldThrottle();
      if (recommendation.getQuotaName() == QuotaName.READ_CAPACITY_UNIT
          || recommendation.getQuotaName() == QuotaName.WRITE_CAPACITY_UNIT) {
        currentQuotaShouldThrottle &= quotaConfig.requestThrottlingEnabled;
      } else if (recommendation.getQuotaName() == QuotaName.STORAGE_IN_GB) {
        currentQuotaShouldThrottle &= quotaConfig.storageQuotaConfig.shouldThrottle;
      }
      shouldThrottle = shouldThrottle | currentQuotaShouldThrottle;
      quotaUsagePercentage.put(recommendation.getQuotaName(), recommendation.getQuotaUsagePercentage());
      recommendedHttpStatus = QuotaUtils.quotaRecommendedHttpResponse(shouldThrottle);
      retryAfterMs = Math.max(recommendation.getRetryAfterMs(), retryAfterMs);
    }
    QuotaUsageLevel quotaUsageLevel = quotaUsagePercentage.isEmpty() ? DEFAULT_QUOTA_USAGE_LEVEL
        : computeWarningLevel(Collections.max(quotaUsagePercentage.values()));
    return new ThrottlingRecommendation(shouldThrottle, quotaUsagePercentage, recommendedHttpStatus, retryAfterMs,
        quotaUsageLevel);
  }

  /**
   * Compute the warning level from usage percentage.
   * @param usagePercentage usage of quota.
   * @return QuotaWarningLevel object.
   */
  private QuotaUsageLevel computeWarningLevel(float usagePercentage) {
    if (usagePercentage >= 100) {
      return QuotaUsageLevel.EXCEEDED;
    }
    if (usagePercentage >= CRITICAL_USAGE_LEVEL_LIMIT) {
      return QuotaUsageLevel.CRITICAL;
    }
    if (usagePercentage >= healthyUsageLevelLimit) {
      return QuotaUsageLevel.WARNING;
    }
    return QuotaUsageLevel.HEALTHY;
  }
}
