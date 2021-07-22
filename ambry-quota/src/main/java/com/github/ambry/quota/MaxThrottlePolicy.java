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
package com.github.ambry.quota;

import com.github.ambry.config.QuotaConfig;
import com.github.ambry.config.StorageQuotaConfig;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * An implementation of {@link ThrottlePolicy} that creates a {@link ThrottlingRecommendation} to throttle if any one of
 * {@link QuotaRecommendation} recommendation is to throttle, and takes the max of retry after time interval. Also
 * groups the quota usage for all the quotas.
 *
 * This Policy also respect the settings in the {@link QuotaConfig}. If the {@link QuotaConfig#requestThrottlingEnabled} is
 * false, we don't throttle on {@link QuotaName#READ_CAPACITY_UNIT} and {@link QuotaName#WRITE_CAPACITY_UNIT}. If the
 * {@link StorageQuotaConfig#shouldThrottle} is false, we don't throttle on {@link QuotaName#STORAGE_IN_GB}.
 */
public class MaxThrottlePolicy implements ThrottlePolicy {
  static final long DEFAULT_RETRY_AFTER_MS = ThrottlingRecommendation.NO_RETRY_AFTER_MS;
  static final int DEFAULT_RECOMMENDED_HTTP_STATUS = HttpResponseStatus.OK.code();
  static final QuotaUsageLevel DEFAULT_QUOTA_USAGE_LEVEL = QuotaUsageLevel.HEALTHY;

  // Percentage usage at or below this limit is healthy.
  static final int HEALTHY_USAGE_LEVEL_LIMIT = 80;
  // Percentage usage level at or below this limit and above healthy limit will generate warning.
  static final int WARNING_USAGE_LEVEL_LIMIT = 95;
  // Percentage usage level at or below this limit and above warning limit will generate critical warning. Anything above will be fatal.
  static final int CRITICAL_USAGE_LEVEL_LIMIT = 100;

  private final QuotaConfig quotaConfig;

  /**
   * Constructor to create a {@link MaxThrottlePolicy}.
   * @param quotaConfig The {@link QuotaConfig}.
   */
  public MaxThrottlePolicy(QuotaConfig quotaConfig) {
    this.quotaConfig = quotaConfig;
  }

  @Override
  public ThrottlingRecommendation recommend(List<QuotaRecommendation> quotaRecommendations) {
    boolean shouldThrottle = false;
    Map<QuotaName, Float> quotaUsagePercentage = new HashMap<>();
    int recommendedHttpStatus = DEFAULT_RECOMMENDED_HTTP_STATUS;
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
      recommendedHttpStatus = Math.max(recommendation.getRecommendedHttpStatus(), recommendedHttpStatus);
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
    if (usagePercentage >= CRITICAL_USAGE_LEVEL_LIMIT) {
      return QuotaUsageLevel.EXCEEDED;
    }
    if (usagePercentage >= WARNING_USAGE_LEVEL_LIMIT) {
      return QuotaUsageLevel.CRITICAL;
    }
    if (usagePercentage >= HEALTHY_USAGE_LEVEL_LIMIT) {
      return QuotaUsageLevel.WARNING;
    }
    return QuotaUsageLevel.HEALTHY;
  }
}
