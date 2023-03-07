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
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;


/**
 * Expose metrics related to writes and reads to/from MySql DB.
 */
public class MySqlMetrics {

  public static final String WRITE_TIME_MSEC = "WriteTimeInMs";
  public static final String WRITE_SUCCESS_COUNT = "WriteSuccessCount";
  public static final String WRITE_FAILURE_COUNT = "WriteFailureCount";
  public static final String READ_TIME_MSEC = "ReadTimeInMs";
  public static final String READ_SUCCESS_COUNT = "ReadSuccessCount";
  public static final String READ_FAILURE_COUNT = "ReadFailureCount";
  public static final String COPY_TIME_MSEC = "CopyTimeInMs";
  public static final String COPY_SUCCESS_COUNT = "CopySuccessCount";
  public static final String COPY_FAILURE_COUNT = "CopyFailureCount";
  public static final String BATCH_UPDATE_TIME_MSEC = "BatchUpdateTimeInMs";
  public static final String BATCH_UPDATE_SUCCESS_COUNT = "BatchUpdateSuccessCount";
  public static final String BATCH_UPDATE_FAILURE_COUNT = "BatchUpdateFailureCount";
  public static final String DELETE_TIME_MSEC = "DeleteTimeInMs";
  public static final String DELETE_SUCCESS_COUNT = "DeleteSuccessCount";
  public static final String DELETE_FAILURE_COUNT = "DeleteFailureCount";
  public static final String CONNECTION_SUCCESS_COUNT = "ConnectionSuccessCount";
  public static final String CONNECTION_FAILURE_COUNT = "ConnectionFailureCount";

  public final Histogram writeTimeMs;
  public final Counter writeSuccessCount;
  public final Counter writeFailureCount;

  public final Histogram readTimeMs;
  public final Counter readSuccessCount;
  public final Counter readFailureCount;

  public final Histogram copyTimeMs;
  public final Counter copySuccessCount;
  public final Counter copyFailureCount;

  public final Histogram batchUpdateTimeMs;
  public final Counter batchUpdateSuccessCount;
  public final Counter batchUpdateFailureCount;

  public final Histogram deleteTimeMs;
  public final Counter deleteSuccessCount;
  public final Counter deleteFailureCount;

  public final Counter connectionSuccessCount;
  public final Counter connectionFailureCount;

  public MySqlMetrics(Class<?> clazz, MetricRegistry metricRegistry) {
    writeTimeMs = metricRegistry.histogram(MetricRegistry.name(clazz, WRITE_TIME_MSEC));
    readTimeMs = metricRegistry.histogram(MetricRegistry.name(clazz, READ_TIME_MSEC));
    copyTimeMs = metricRegistry.histogram(MetricRegistry.name(clazz, COPY_TIME_MSEC));
    batchUpdateTimeMs = metricRegistry.histogram(MetricRegistry.name(clazz, BATCH_UPDATE_TIME_MSEC));
    deleteTimeMs = metricRegistry.histogram(MetricRegistry.name(clazz, DELETE_TIME_MSEC));
    writeSuccessCount = metricRegistry.counter(MetricRegistry.name(clazz, WRITE_SUCCESS_COUNT));
    writeFailureCount = metricRegistry.counter(MetricRegistry.name(clazz, WRITE_FAILURE_COUNT));
    readSuccessCount = metricRegistry.counter(MetricRegistry.name(clazz, READ_SUCCESS_COUNT));
    readFailureCount = metricRegistry.counter(MetricRegistry.name(clazz, READ_FAILURE_COUNT));
    copySuccessCount = metricRegistry.counter(MetricRegistry.name(clazz, COPY_SUCCESS_COUNT));
    copyFailureCount = metricRegistry.counter(MetricRegistry.name(clazz, COPY_FAILURE_COUNT));
    batchUpdateSuccessCount = metricRegistry.counter(MetricRegistry.name(clazz, BATCH_UPDATE_SUCCESS_COUNT));
    batchUpdateFailureCount = metricRegistry.counter(MetricRegistry.name(clazz, BATCH_UPDATE_FAILURE_COUNT));
    deleteSuccessCount = metricRegistry.counter(MetricRegistry.name(clazz, DELETE_SUCCESS_COUNT));
    deleteFailureCount = metricRegistry.counter(MetricRegistry.name(clazz, DELETE_FAILURE_COUNT));
    connectionSuccessCount = metricRegistry.counter(MetricRegistry.name(clazz, CONNECTION_SUCCESS_COUNT));
    connectionFailureCount = metricRegistry.counter(MetricRegistry.name(clazz, CONNECTION_FAILURE_COUNT));
  }
}
