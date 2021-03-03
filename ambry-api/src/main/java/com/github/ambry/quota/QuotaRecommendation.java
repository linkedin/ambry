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
 * Class representing recommendation made by a {@link QuotaEnforcer} implementation. QuotaEnforcer
 * implementations can use this object to provide a boolean recommendation to throttle the
 * request or not, along with usage information like usage percentage, name of the quota for which this recommendation
 * was made and the recommended http status (indicating whether or not throttled request should be retried).
 */
public class QuotaRecommendation {
  private final boolean shouldThrottle;
  private final float usagePercentage;
  private final QuotaName quotaName;
  private final int recommendedHttpStatus;
  private final long retryAfterMs;

  /**
   * Constructor for {@link QuotaRecommendation}.
   * @param shouldThrottle boolean flag indicating throttling recommendation.
   * @param usagePercentage percentage of resource usage.
   * @param quotaName name of the enforcement that made the recommendation.
   * @param recommendedHttpStatus recommended http status.
   * @param retryAfterMs time after which request can be retried.
   */
  public QuotaRecommendation(boolean shouldThrottle, float usagePercentage, QuotaName quotaName,
      int recommendedHttpStatus, long retryAfterMs) {
    this.shouldThrottle = shouldThrottle;
    this.usagePercentage = usagePercentage;
    this.quotaName = quotaName;
    this.recommendedHttpStatus = recommendedHttpStatus;
    this.retryAfterMs = retryAfterMs;
  }

  /**
   * @return true if recommendation is to throttle. false otherwise.
   */
  public boolean shouldThrottle() {
    return shouldThrottle;
  }

  /**
   * @return estimation of percentage of quota in use when the recommendation was made.
   */
  public float getQuotaUsagePercentage() {
    return usagePercentage;
  }

  /**
   * @return name of the quota for which recommendation is made.
   */
  public QuotaName getQuotaName() {
    return quotaName;
  }

  /**
   * @return http status recommended by enforcer.
   */
  public int getRecommendedHttpStatus() {
    return recommendedHttpStatus;
  }

  /**
   * @return the time interval in milliseconds after the request can be retried.
   * If request is not throttled then returns -1.
   */
  public long getRetryAfterMs() {
    return retryAfterMs;
  }
}
