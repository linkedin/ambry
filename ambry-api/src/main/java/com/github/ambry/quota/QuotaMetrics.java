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
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


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
  public final Meter noChargeRate;
  public final Meter partialQuotaRecommendationRate;
  public final Meter noRecommendationRate;
  public final Meter forcedChargeRate;
  public final Meter throttleRate;
  public final Meter noQuotaRecommendationRate;
  public final Meter recommendRate;
  public final Meter chargeAndRecommendRate;
  public final Counter accountUpdateNotificationCount;
  public Counter highSystemResourceUsageCount;
  private final MetricRegistry metricRegistry;
  public Gauge<Integer> frontendUsageWriteAmplificationGauge;
  public Gauge<Integer> frontendUsageReadAmplificationGauge;
  public Map<String, Counter> perQuotaResourceOutOfQuotaMap = new HashMap<>();
  public Map<String, Counter> perQuotaResourceWouldBeThrottledMap = new HashMap<>();
  public Map<String, Counter> perQuotaResourceDelayedRequestMap = new HashMap<>();

  /**
   * {@link QuotaMetrics} constructor.
   * @param metricRegistry {@link MetricRegistry} object.
   */
  public QuotaMetrics(MetricRegistry metricRegistry) {
    this.metricRegistry = metricRegistry;
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
    highSystemResourceUsageCount =
        metricRegistry.counter(MetricRegistry.name(QuotaManager.class, "HighSystemResourceUsageCount"));
    noChargeRate = metricRegistry.meter(MetricRegistry.name(QuotaManager.class, "NoChargeRate"));
    noRecommendationRate = metricRegistry.meter(MetricRegistry.name(QuotaManager.class, "NoRecommendationRate"));
  }

  /**
   * Setup the {@link Gauge}s for read and write amplification factor.
   * Frontend usage cost could get amplified because to serve one copy of data, frontend might need to simultaneously
   * read or write the data over network to multiple storage nodes.
   * @param frontendUsageReadAmplificationFactor read amplification factor.
   * @param frontendUsageWriteAmplificationFactor write amplification factor.
   */
  public void setupFeUsageAmplificationFactorMetrics(final int frontendUsageReadAmplificationFactor,
      final int frontendUsageWriteAmplificationFactor) {
    frontendUsageReadAmplificationGauge =
        metricRegistry.gauge(MetricRegistry.name(QuotaManager.class, "FrontendUsageReadAmplifcationFactor"),
            () -> (Gauge<Integer>) () -> frontendUsageReadAmplificationFactor);
    frontendUsageWriteAmplificationGauge =
        metricRegistry.gauge(MetricRegistry.name(QuotaManager.class, "FrontendUsageWriteAmplifcationFactor"),
            () -> (Gauge<Integer>) () -> frontendUsageWriteAmplificationFactor);
  }

  /**
   * Creates the per quota resource metric to track if there were any out of quota or delayed requests.
   * Note that this method is not thread, and it should be called from a synchronized context.
   * @param quotaResourceIds {@link List} of {@link String} resource ids to track.
   */
  public void createMetricsForQuotaResources(List<String> quotaResourceIds) {
    for (final String quotaResourceId : quotaResourceIds) {
      perQuotaResourceDelayedRequestMap.putIfAbsent(quotaResourceId, metricRegistry.counter(
          MetricRegistry.name(QuotaEnforcer.class,
              String.format("QuotaResource-%s-DelayedRequestCount", quotaResourceId))));
      perQuotaResourceOutOfQuotaMap.putIfAbsent(quotaResourceId, metricRegistry.counter(
          MetricRegistry.name(QuotaEnforcer.class,
              String.format("QuotaResource-%s-OutOfQuotaRequestCount", quotaResourceId))));
      perQuotaResourceWouldBeThrottledMap.putIfAbsent(quotaResourceId, metricRegistry.counter(
          MetricRegistry.name(QuotaEnforcer.class,
              String.format("QuotaResource-%s-WouldBeThrottledCount", quotaResourceId))));
    }
  }

  /**
   * @return MetricRegistry object.
   */
  public MetricRegistry getMetricRegistry() {
    return metricRegistry;
  }
}
