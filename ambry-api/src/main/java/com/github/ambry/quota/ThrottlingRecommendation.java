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

import io.netty.handler.codec.http.HttpResponseStatus;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


/**
 * Class that returns the overall throttling recommendation for all the quotas.
 */
public class ThrottlingRecommendation {
  public static final long NO_RETRY_AFTER_MS = -1; // No retry needed when request is not throttled.
  private final boolean throttle;
  private final Map<QuotaName, Float> quotaUsagePercentage;
  private final HttpResponseStatus recommendedHttpStatus;
  private final long retryAfterMs;
  private final QuotaUsageLevel quotaUsageLevel;

  /**
   * Constructor for {@link ThrottlingRecommendation}.
   * @param throttle flag indicating if request should be throttled.
   * @param quotaUsagePercentage A {@link Map} of {@link QuotaName} to usage percentage.
   * @param recommendedHttpStatus {@link HttpResponseStatus} representing overall recommended http status.
   * @param retryAfterMs time in ms after which request should be retried. -1 if request is not throttled.
   * @param quotaUsageLevel {@link QuotaUsageLevel} object.
   */
  public ThrottlingRecommendation(boolean throttle, Map<QuotaName, Float> quotaUsagePercentage,
      HttpResponseStatus recommendedHttpStatus, long retryAfterMs, QuotaUsageLevel quotaUsageLevel) {
    this.throttle = throttle;
    this.quotaUsagePercentage = new HashMap<>(quotaUsagePercentage);
    this.recommendedHttpStatus = recommendedHttpStatus;
    this.retryAfterMs = retryAfterMs;
    this.quotaUsageLevel = quotaUsageLevel;
  }

  /**
   * @return true if recommendation is to throttle. false otherwise.
   */
  public boolean shouldThrottle() {
    return this.throttle;
  }

  /**
   * @return A {@link Map} of quota  name and estimation of percentage of quota in use when the recommendation was made.
   */
  public Map<QuotaName, Float> getQuotaUsagePercentage() {
    return Collections.unmodifiableMap(this.quotaUsagePercentage);
  }

  /**
   * @return http status recommended.
   */
  public HttpResponseStatus getRecommendedHttpStatus() {
    return this.recommendedHttpStatus;
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
}
