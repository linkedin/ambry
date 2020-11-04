/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.account.mysql;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;


/**
 * Expose metrics related to writes and reads to/from MySql DB used in {@link com.github.ambry.account.MySqlAccountService}
 */
public class MySqlAccountStoreMetrics {

  public final Histogram writeTimeInMs;
  public final Histogram readTimeInMs;
  public final Counter writeSuccessCount;
  public final Counter writeFailureCount;
  public final Counter readSuccessCount;
  public final Counter readFailureCount;

  public MySqlAccountStoreMetrics(MetricRegistry metricRegistry) {
    writeTimeInMs = metricRegistry.histogram(MetricRegistry.name(MySqlAccountStore.class, "WriteTimeInMs"));
    readTimeInMs = metricRegistry.histogram(MetricRegistry.name(MySqlAccountStore.class, "ReadTimeInMs"));
    writeSuccessCount = metricRegistry.counter(MetricRegistry.name(MySqlAccountStore.class, "WriteSuccessCount"));
    writeFailureCount = metricRegistry.counter(MetricRegistry.name(MySqlAccountStore.class, "WriteFailureCount"));
    readSuccessCount = metricRegistry.counter(MetricRegistry.name(MySqlAccountStore.class, "ReadSuccessCount"));
    readFailureCount = metricRegistry.counter(MetricRegistry.name(MySqlAccountStore.class, "ReadFailureCount"));
  }
}
