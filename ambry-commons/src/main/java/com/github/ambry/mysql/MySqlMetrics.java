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
package com.github.ambry.mysql;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;


/**
 * Expose metrics related to writes and reads to/from MySql DB.
 */
public class MySqlMetrics {

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

  public MySqlMetrics(Class<?> clazz, MetricRegistry metricRegistry) {
    writeTimeMs = metricRegistry.histogram(MetricRegistry.name(clazz, WRITE_TIME_MSEC));
    readTimeMs = metricRegistry.histogram(MetricRegistry.name(clazz, READ_TIME_MSEC));
    writeSuccessCount = metricRegistry.counter(MetricRegistry.name(clazz, WRITE_SUCCESS_COUNT));
    writeFailureCount = metricRegistry.counter(MetricRegistry.name(clazz, WRITE_FAILURE_COUNT));
    readSuccessCount = metricRegistry.counter(MetricRegistry.name(clazz, READ_SUCCESS_COUNT));
    readFailureCount = metricRegistry.counter(MetricRegistry.name(clazz, READ_FAILURE_COUNT));
    connectionSuccessCount = metricRegistry.counter(MetricRegistry.name(clazz, CONNECTION_SUCCESS_COUNT));
    connectionFailureCount = metricRegistry.counter(MetricRegistry.name(clazz, CONNECTION_FAILURE_COUNT));
  }
}
