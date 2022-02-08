/*
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;


/**
 * Metrics class to capture metrics for request quota enforcement.
 */
public class QuotaMetrics {
  public final Meter quotaExceedRecommendationRate;
  public final Timer quotaRecommendationTime;
  public final Meter quotaNotChargedRate;
  public final Timer quotaManagerInitTime;
  public final Timer quotaChargeTime;
  public final Counter quotaResourceInvalidUnlockAttemptCount;
  public final Meter quotaExceedAllowedRate;
  public final Meter partialChargeRate;
  public final Meter partialQuotaRecommendationRate;
  public final Meter forcedChargeRate;
  public final Meter throttleRate;
  public final Meter noQuotaRecommendationRate;
  public final Meter recommendRate;
  public final Meter chargeAndRecommendRate;
  public final Counter accountUpdateNotificationCount;

  /**
   * {@link QuotaMetrics} constructor.
   * @param metricRegistry {@link MetricRegistry} object.
   */
  public QuotaMetrics(MetricRegistry metricRegistry) {
    quotaExceedRecommendationRate =
        metricRegistry.meter(MetricRegistry.name(QuotaMetrics.class, "QuotaExceedRecommendationRate"));
    quotaRecommendationTime = metricRegistry.timer(MetricRegistry.name(QuotaMetrics.class, "QuotaRecommendationTime"));
    quotaNotChargedRate = metricRegistry.meter(MetricRegistry.name(QuotaMetrics.class, "QuotaNotChargedRate"));
    quotaManagerInitTime = metricRegistry.timer(MetricRegistry.name(QuotaMetrics.class, "QuotaManagerInitTime"));
    quotaChargeTime = metricRegistry.timer(MetricRegistry.name(QuotaMetrics.class, "QuotaChargeTime"));
    quotaResourceInvalidUnlockAttemptCount =
        metricRegistry.counter(MetricRegistry.name(QuotaMetrics.class, "QuotaResourceInvalidUnlockAttemptCount"));
    quotaExceedAllowedRate = metricRegistry.meter(MetricRegistry.name(QuotaManager.class, "QuotaExceedAllowedRate"));
    partialChargeRate = metricRegistry.meter(MetricRegistry.name(QuotaManager.class, "PartialChargeRate"));
    partialQuotaRecommendationRate =
        metricRegistry.meter(MetricRegistry.name(QuotaManager.class, "PartialQuotaRecommendationRate"));
    forcedChargeRate = metricRegistry.meter(MetricRegistry.name(QuotaManager.class, "ForcedChargeRate"));
    throttleRate = metricRegistry.meter(MetricRegistry.name(QuotaManager.class, "ThrottleRate"));
    noQuotaRecommendationRate =
        metricRegistry.meter(MetricRegistry.name(QuotaManager.class, "NoQuotaRecommendationRate"));
    recommendRate = metricRegistry.meter(MetricRegistry.name(QuotaManager.class, "RecommendRate"));
    chargeAndRecommendRate = metricRegistry.meter(MetricRegistry.name(QuotaManager.class, "ChargeAndRecommendRate"));
    accountUpdateNotificationCount =
        metricRegistry.counter(MetricRegistry.name(QuotaManager.class, "AccountUpdateNotificationCount"));
  }
}
