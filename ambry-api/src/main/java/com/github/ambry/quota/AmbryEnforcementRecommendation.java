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

/**
 * An implementation of {@link EnforcementRecommendation} made by Ambry.
 */
public class AmbryEnforcementRecommendation implements EnforcementRecommendation {
  private final boolean shouldThrottle;
  private final float usagePercentage;
  private final QuotaName quotaName;
  private final int recommendedHttpStatus;
  private final double requestCost;
  private final long retryAfterMs;

  /**
   * Constructor for {@link AmbryEnforcementRecommendation}.
   * @param shouldThrottle boolean flag indicating throttling recommendation.
   * @param usagePercentage percentage of resource usage.
   * @param quotaName name of the enforcement that made the recommendation.
   * @param recommendedHttpStatus recommended http status.
   * @param requestCost cost of the request.
   * @param retryAfterMs time after which request can be retried.
   */
  public AmbryEnforcementRecommendation(boolean shouldThrottle, float usagePercentage, QuotaName quotaName,
      int recommendedHttpStatus, float requestCost, long retryAfterMs) {
    this.shouldThrottle = shouldThrottle;
    this.usagePercentage = usagePercentage;
    this.quotaName = quotaName;
    this.recommendedHttpStatus = recommendedHttpStatus;
    this.requestCost = requestCost;
    this.retryAfterMs = retryAfterMs;
  }

  @Override
  public boolean shouldThrottle() {
    return shouldThrottle;
  }

  @Override
  public float getQuotaUsagePercentage() {
    return usagePercentage;
  }

  @Override
  public QuotaName getQuotaName() {
    return quotaName;
  }

  @Override
  public int getRecommendedHttpStatus() {
    return recommendedHttpStatus;
  }

  @Override
  public double getRequestCost() {
    return requestCost;
  }

  @Override
  public long getRetryAfterMs() {
    return retryAfterMs;
  }
}
