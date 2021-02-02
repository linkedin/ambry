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

import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * An implementation of {@link ThrottlePolicy} that creates a {@link ThrottlingRecommendation} to throttle if any one of
 * {@link EnforcementRecommendation} recommendation is to throttle, and takes the max of retry after time interval. Also
 * groups the quota usage and request cost for all the quotas.
 */
public class MaxThrottlePolicy implements ThrottlePolicy {
  @Override
  public ThrottlingRecommendation recommend(List<EnforcementRecommendation> enforcementRecommendations) {
    boolean shouldThrottle = false;
    Map<QuotaName, Float> quotaUsagePercentage = new HashMap<>();
    int recommendedHttpStatus = 200;
    Map<QuotaName, Double> requestCost = new HashMap<>();
    long retryAfterMs = -1;
    for (EnforcementRecommendation recommendation : enforcementRecommendations) {
      shouldThrottle = shouldThrottle | recommendation.shouldThrottle();
      quotaUsagePercentage.put(recommendation.getQuotaName(), recommendation.getQuotaUsagePercentage());
      recommendedHttpStatus = Math.max(recommendation.getRecommendedHttpStatus(), recommendedHttpStatus);
      requestCost.put(recommendation.getQuotaName(), recommendation.getRequestCost());
      retryAfterMs = Math.max(recommendation.getRetryAfterMs(), retryAfterMs);
    }
    return new ThrottlingRecommendation(shouldThrottle, quotaUsagePercentage, recommendedHttpStatus, requestCost,
        retryAfterMs);
  }
}
