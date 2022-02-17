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
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;


/**
 * Metrics class to capture metrics for request quota enforcement.
 */
public class QuotaMetrics {
  public final Counter quotaExceededCount;
  public final Timer quotaEnforcementTime;
  public final Counter quotaNotEnforcedCount;
  public final Timer quotaManagerInitTime;
  public final Timer quotaChargeTime;
  public final Counter quotaResourceInvalidUnlockAttemptCount;

  /**
   * {@link QuotaMetrics} constructor.
   * @param metricRegistry {@link MetricRegistry} object.
   */
  public QuotaMetrics(MetricRegistry metricRegistry) {
    quotaExceededCount = metricRegistry.counter(MetricRegistry.name(QuotaMetrics.class, "QuotaExceededCount"));
    quotaEnforcementTime = metricRegistry.timer(MetricRegistry.name(QuotaMetrics.class, "QuotaEnforcementTime"));
    quotaNotEnforcedCount = metricRegistry.counter(MetricRegistry.name(QuotaMetrics.class, "QuotaNotEnforcedCount"));
    quotaManagerInitTime = metricRegistry.timer(MetricRegistry.name(QuotaMetrics.class, "QuotaManagerInitTime"));
    quotaChargeTime = metricRegistry.timer(MetricRegistry.name(QuotaMetrics.class, "QuotaChargeTime"));
    quotaResourceInvalidUnlockAttemptCount =
        metricRegistry.counter(MetricRegistry.name(QuotaMetrics.class, "QuotaResourceInvalidUnlockAttemptCount"));
  }
}
