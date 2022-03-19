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

/**
 * Class representing recommendation made by a {@link QuotaEnforcer} implementation. QuotaEnforcer
 * implementations can use this object to specify a {@link QuotaAction}, as action recommended to ensure quota
 * compliance, along with percentage usage and name of the quota for which this recommendation was made.
 */
public class QuotaRecommendation {
  private final QuotaAction quotaAction;
  private final float usagePercentage;
  private final QuotaName quotaName;
  private final long retryAfterMs;
  public final static long NO_THROTTLE_RETRY_AFTER_MS = -1;

  /**
   * Constructor for {@link QuotaRecommendation}.
   * @param quotaAction the {@link QuotaAction} recommended.
   * @param usagePercentage percentage of resource usage.
   * @param quotaName name of the enforcement that made the recommendation.
   * @param retryAfterMs time after which request can be retried.
   */
  public QuotaRecommendation(QuotaAction quotaAction, float usagePercentage, QuotaName quotaName, long retryAfterMs) {
    this.quotaAction = quotaAction;
    this.usagePercentage = usagePercentage;
    this.quotaName = quotaName;
    this.retryAfterMs = retryAfterMs;
  }

  /**
   * @return QuotaAction object representing the recommended action.
   */
  public QuotaAction getQuotaAction() {
    return quotaAction;
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
   * @return the time interval in milliseconds after the request can be retried.
   * If request is not throttled then returns NO_THROTTLE_RETRY_AFTER_MS.
   */
  public long getRetryAfterMs() {
    return retryAfterMs;
  }
}
