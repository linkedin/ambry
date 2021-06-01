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
package com.github.ambry.quota.storage;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;


/**
 * Metrics class to record metrics for storage quota service components.
 */
public class StorageQuotaServiceMetrics {
  public final Counter quotaExceededCount;
  public final Histogram quotaExceededCallbackTimeMs;
  public final Histogram shouldThrottleTimeMs;
  public final Histogram mysqlRefresherInitTimeMs;
  public final Histogram mysqlRefresherRefreshUsageTimeMs;

  public StorageQuotaServiceMetrics(MetricRegistry register) {
    quotaExceededCount = register.counter(MetricRegistry.name(StorageQuotaEnforcer.class, "QuotaExceededCount"));
    quotaExceededCallbackTimeMs =
        register.histogram(MetricRegistry.name(StorageQuotaEnforcer.class, "QuotaExceededCallbackTimeMs"));
    shouldThrottleTimeMs = register.histogram(MetricRegistry.name(StorageQuotaEnforcer.class, "ShouldThrottleTimeMs"));
    mysqlRefresherInitTimeMs =
        register.histogram(MetricRegistry.name(StorageQuotaEnforcer.class, "MysqlRefresherInitTimeMs"));
    mysqlRefresherRefreshUsageTimeMs =
        register.histogram(MetricRegistry.name(StorageQuotaEnforcer.class, "MysqlRefresherRefreshUsageTimeMs"));
  }
}
