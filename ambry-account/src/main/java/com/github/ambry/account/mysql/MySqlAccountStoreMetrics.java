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

  public static final String WRITE_TIME_MSEC = "WriteTimeInMs";
  public static final String READ_TIME_MSEC = "ReadTimeInMs";
  public static final String WRITE_SUCCESS_COUNT = "WriteSuccessCount";
  public static final String WRITE_FAILURE_COUNT = "WriteFailureCount";
  public static final String READ_SUCCESS_COUNT = "ReadSuccessCount";
  public static final String READ_FAILURE_COUNT = "ReadFailureCount";
  public static final String CONNECTION_SUCCESS_COUNT = "ConnectionSuccessCount";
  public static final String CONNECTION_FAILURE_COUNT = "ConnectionFailureCount";

  public final Histogram writeTimeMs;
  public final Histogram readTimeMs;
  public final Counter writeSuccessCount;
  public final Counter writeFailureCount;
  public final Counter readSuccessCount;
  public final Counter readFailureCount;
  public final Counter connectionSuccessCount;
  public final Counter connectionFailureCount;

  public MySqlAccountStoreMetrics(MetricRegistry metricRegistry) {
    writeTimeMs = metricRegistry.histogram(MetricRegistry.name(MySqlAccountStore.class, WRITE_TIME_MSEC));
    readTimeMs = metricRegistry.histogram(MetricRegistry.name(MySqlAccountStore.class, READ_TIME_MSEC));
    writeSuccessCount = metricRegistry.counter(MetricRegistry.name(MySqlAccountStore.class, WRITE_SUCCESS_COUNT));
    writeFailureCount = metricRegistry.counter(MetricRegistry.name(MySqlAccountStore.class, WRITE_FAILURE_COUNT));
    readSuccessCount = metricRegistry.counter(MetricRegistry.name(MySqlAccountStore.class, READ_SUCCESS_COUNT));
    readFailureCount = metricRegistry.counter(MetricRegistry.name(MySqlAccountStore.class, READ_FAILURE_COUNT));
    connectionSuccessCount = metricRegistry.counter(MetricRegistry.name(MySqlAccountStore.class, CONNECTION_SUCCESS_COUNT));
    connectionFailureCount = metricRegistry.counter(MetricRegistry.name(MySqlAccountStore.class, CONNECTION_FAILURE_COUNT));
  }
}
