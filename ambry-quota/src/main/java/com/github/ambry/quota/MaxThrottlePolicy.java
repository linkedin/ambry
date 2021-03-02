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

import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * An implementation of {@link ThrottlePolicy} that creates a {@link ThrottlingRecommendation} to throttle if any one of
 * {@link QuotaRecommendation} recommendation is to throttle, and takes the max of retry after time interval. Also
 * groups the quota usage for all the quotas.
 */
public class MaxThrottlePolicy implements ThrottlePolicy {
  static final long DEFAULT_RETRY_AFTER_MS = ThrottlingRecommendation.NO_RETRY_VALUE_FOR_RETRY_AFTER_MS;
  static final int DEFAULT_RECOMMENDED_HTTP_STATUS = HttpResponseStatus.OK.code();
  static final QuotaWarningLevel DEFAULT_QUOTA_WARNING_LEVEL = QuotaWarningLevel.HEALTHY;
  static final boolean DEFAULT_SHOULD_THROTTLE = false;

  // Percentage usage at or below this limit is healthy.
  static final int HEALTHY_USAGE_LEVEL_LIMIT = 80;
  // Percentage usage level at or below this limit and above healthy limit will generate warning.
  static final int WARNING_USAGE_LEVEL_LIMIT = 95;
  // Percentage usage level at or below this limit and above warning limit will generate critical warning. Anything above will be fatal.
  static final int CRITICAL_USAGE_LEVEL_LIMIT = 100;

  @Override
  public ThrottlingRecommendation recommend(List<QuotaRecommendation> quotaRecommendations) {
    boolean shouldThrottle = false;
    Map<QuotaName, Float> quotaUsagePercentage = new HashMap<>();
    int recommendedHttpStatus = DEFAULT_RECOMMENDED_HTTP_STATUS;
    long retryAfterMs = DEFAULT_RETRY_AFTER_MS;
    QuotaWarningLevel quotaWarningLevel = DEFAULT_QUOTA_WARNING_LEVEL;
    for (QuotaRecommendation recommendation : quotaRecommendations) {
      shouldThrottle = shouldThrottle | recommendation.shouldThrottle();
      quotaUsagePercentage.put(recommendation.getQuotaName(), recommendation.getQuotaUsagePercentage());
      recommendedHttpStatus = Math.max(recommendation.getRecommendedHttpStatus(), recommendedHttpStatus);
      retryAfterMs = Math.max(recommendation.getRetryAfterMs(), retryAfterMs);
      quotaWarningLevel = QuotaWarningLevel.fromInt(Math.max(quotaWarningLevel.ordinal(),
          computeWarningLevel(recommendation.getQuotaUsagePercentage()).ordinal()));
    }
    return new ThrottlingRecommendation(shouldThrottle, quotaUsagePercentage, recommendedHttpStatus, retryAfterMs,
        quotaWarningLevel);
  }

  /**
   * Compute the warning level from usage percentage.
   * @param usagePercentage usage of quota.
   * @return QuotaWarningLevel object.
   */
  private QuotaWarningLevel computeWarningLevel(float usagePercentage) {
    if (usagePercentage >= CRITICAL_USAGE_LEVEL_LIMIT) {
      return QuotaWarningLevel.FATAL;
    }
    if (usagePercentage >= WARNING_USAGE_LEVEL_LIMIT) {
      return QuotaWarningLevel.CRITICAL;
    }
    if (usagePercentage >= HEALTHY_USAGE_LEVEL_LIMIT) {
      return QuotaWarningLevel.WARNING;
    }
    return QuotaWarningLevel.HEALTHY;
  }
}
