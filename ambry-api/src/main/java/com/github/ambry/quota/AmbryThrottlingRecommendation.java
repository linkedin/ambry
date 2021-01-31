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
import java.util.Map;


/**
 *
 */
public class AmbryThrottlingRecommendation implements ThrottlingRecommendation {
  private final boolean throttle;
  private final Map<QuotaName, Float> quotaUsagePercentage;
  private final int recommendedHttpStatus;
  private final Map<QuotaName, Double> requestCost;
  private final long retryAfterMs;

  /**
   * Constructor for {@link AmbryThrottlingRecommendation}.
   * @param throttle
   * @param quotaUsagePercentage
   * @param recommendedHttpStatus
   * @param requestCost
   * @param retryAfterMs
   */
  public AmbryThrottlingRecommendation(boolean throttle, Map<QuotaName, Float> quotaUsagePercentage,
      int recommendedHttpStatus, Map<QuotaName, Double> requestCost, long retryAfterMs) {
    this.throttle = throttle;
    this.quotaUsagePercentage = new HashMap<>(quotaUsagePercentage);
    this.recommendedHttpStatus = recommendedHttpStatus;
    this.requestCost = new HashMap<>(requestCost);
    this.retryAfterMs = retryAfterMs;
  }

  @Override
  public boolean shouldThrottle() {
    return false;
  }

  @Override
  public Map<QuotaName, Float> getQuotaUsagePercentage() {
    return null;
  }

  @Override
  public int getRecommendedHttpStatus() {
    return 0;
  }

  @Override
  public Map<QuotaName, Double> getRequestCost() {
    return null;
  }

  @Override
  public long getRetryAfterMs() {
    return 0;
  }
}
