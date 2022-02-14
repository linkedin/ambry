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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


/**
 * Class that returns the overall throttling recommendation for all the quotas.
 */
public class ThrottlingRecommendation {
  public static final long NO_RETRY_AFTER_MS = -1; // No retry needed when request is not throttled.
  private final QuotaAction quotaAction;
  private final Map<QuotaName, Float> quotaUsagePercentage;
  private final long retryAfterMs;
  private final QuotaUsageLevel quotaUsageLevel;

  /**
   * Constructor for {@link ThrottlingRecommendation}.
   * @param quotaAction {@link QuotaAction} object as the recommended action to take.
   * @param quotaUsagePercentage A {@link Map} of {@link QuotaName} to usage percentage.
   * @param retryAfterMs time in ms after which request should be retried. -1 if request is not throttled.
   * @param quotaUsageLevel {@link QuotaUsageLevel} object.
   */
  public ThrottlingRecommendation(QuotaAction quotaAction, Map<QuotaName, Float> quotaUsagePercentage,
      long retryAfterMs, QuotaUsageLevel quotaUsageLevel) {
    this.quotaAction = quotaAction;
    this.quotaUsagePercentage = new HashMap<>(quotaUsagePercentage);
    this.retryAfterMs = retryAfterMs;
    this.quotaUsageLevel = quotaUsageLevel;
  }

  /**
   * @return true if recommendation is to throttle. false otherwise.
   */
  public boolean shouldThrottle() {
    return this.quotaAction == QuotaAction.REJECT || this.quotaAction == QuotaAction.DELAY;
  }

  /**
   * @return true if recommendation is to delay request. false otherwise.
   */
  public boolean shouldDelay() {
    return this.quotaAction == QuotaAction.DELAY;
  }

  /**
   * @return true if recommendation is to reject request. false otherwise.
   */
  public boolean shouldReject() {
    return this.quotaAction == QuotaAction.REJECT;
  }

  /**
   * @return A {@link Map} of quota  name and estimation of percentage of quota in use when the recommendation was made.
   */
  public Map<QuotaName, Float> getQuotaUsagePercentage() {
    return Collections.unmodifiableMap(this.quotaUsagePercentage);
  }

  /**
   * @return the time interval in milliseconds after the request can be retried.
   */
  public long getRetryAfterMs() {
    return this.retryAfterMs;
  }

  /**
   * @return QuotaUsageLevel based on usage.
   */
  public QuotaUsageLevel getQuotaUsageLevel() {
    return quotaUsageLevel;
  }

  /**
   * @return QuotaAction object.
   */
  public QuotaAction getQuotaAction() {
    return quotaAction;
  }
}
