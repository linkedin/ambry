/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.router;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * {@link CoordinatorBackedRouter} specific metrics tracking.
 * <p/>
 * Exports metrics that are triggered by the admin to the provided {@link MetricRegistry}.
 */
public class CoordinatorBackedRouterMetrics {

  // Rates
  // CoordinatorBackedRouter
  public final Meter deleteBlobRate;
  public final Meter getBlobInfoRate;
  public final Meter getBlobRate;
  public final Meter putBlobRate;
  // CoordinatorOperation
  public final Meter operationQueuingRate;
  public final Meter operationDequeuingRate;

  // Latencies
  // CoordinatorBackedRouter
  public final Histogram closeTimeInMs;
  public final Histogram operationPostProcessingTimeInMs;
  public final Histogram operationPreProcessingTimeInMs;
  // CoordinatorOperation
  public final Histogram operationQueuingTimeInMs;
  // DeleteBlob
  public final Histogram deleteBlobTotalTimeInMs;
  // GetBlob
  public final Histogram getBlobResultConstructionTimeInMs;
  public final Histogram getBlobTimeInMs;
  public final Histogram getBlobTotalTimeInMs;
  // GetBlobInfo
  public final Histogram getBlobInfoResultConstructionTimeInMs;
  public final Histogram getBlobInfoTotalTimeInMs;
  public final Histogram getBlobPropertiesTimeInMs;
  public final Histogram getUserMetadataTimeInMs;
  // PutBlob
  public final Histogram putBlobTotalTimeInMs;

  // Errors
  // CoordinatorBackedRouter
  public final Counter closeError;
  public final Counter futureCallbackError;
  public final Counter unavailableError;
  // CoordinatorOperation
  public final Counter operationError;
  public final Counter queueStartTimeNotRecordedError;

  public final AtomicInteger totalOperationsInFlight = new AtomicInteger(0);
  public final AtomicInteger totalOperationsInExecution = new AtomicInteger(0);

  /**
   * Creates an instance of CoordinatorBackedRouterMetrics using the given {@code metricRegistry}.
   * @param metricRegistry the {@link MetricRegistry} to use for the metrics.
   */
  public CoordinatorBackedRouterMetrics(MetricRegistry metricRegistry) {
    // Rates
    // CoordinatorBackedRouter
    deleteBlobRate = metricRegistry.meter(MetricRegistry.name(CoordinatorBackedRouter.class, "DeleteBlobRate"));
    getBlobInfoRate = metricRegistry.meter(MetricRegistry.name(CoordinatorBackedRouter.class, "GetBlobInfoRate"));
    getBlobRate = metricRegistry.meter(MetricRegistry.name(CoordinatorBackedRouter.class, "GetBlobRate"));
    putBlobRate = metricRegistry.meter(MetricRegistry.name(CoordinatorBackedRouter.class, "PutBlobRate"));
    // CoordinatorOperation
    operationQueuingRate =
        metricRegistry.meter(MetricRegistry.name(CoordinatorOperation.class, "OperationQueuingRate"));
    operationDequeuingRate =
        metricRegistry.meter(MetricRegistry.name(CoordinatorOperation.class, "OperationDequeuingRate"));

    // Latencies
    // CoordinatorBackedRouter
    closeTimeInMs = metricRegistry.histogram(MetricRegistry.name(CoordinatorBackedRouter.class, "CloseTimeInMs"));
    operationPostProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(CoordinatorBackedRouter.class, "OperationPostProcessingTimeInMs"));
    operationPreProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(CoordinatorBackedRouter.class, "OperationPreProcessingTimeInMs"));
    // CoordinatorOperation
    operationQueuingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(CoordinatorOperation.class, "OperationQueuingTimeInMs"));
    // DeleteBlob
    deleteBlobTotalTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(CoordinatorOperation.class, "DeleteBlobTotalTimeInMs"));
    // GetBlob
    getBlobResultConstructionTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(CoordinatorOperation.class, "GetBlobResultConstructionTimeInMs"));
    getBlobTimeInMs = metricRegistry.histogram(MetricRegistry.name(CoordinatorOperation.class, "GetBlobTimeInMs"));
    getBlobTotalTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(CoordinatorOperation.class, "GetBlobTotalTimeInMs"));
    // GetBlobInfo
    getBlobInfoResultConstructionTimeInMs = metricRegistry
        .histogram(MetricRegistry.name(CoordinatorOperation.class, "GetBlobInfoResultConstructionTimeInMs"));
    getBlobInfoTotalTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(CoordinatorOperation.class, "GetBlobInfoTotalTimeInMs"));
    getBlobPropertiesTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(CoordinatorOperation.class, "GetBlobPropertiesTimeInMs"));
    getUserMetadataTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(CoordinatorOperation.class, "GetUserMetadataTimeInMs"));
    // PutBlob
    putBlobTotalTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(CoordinatorOperation.class, "PutBlobTotalTimeInMs"));

    // Errors
    // CoordinatorBackedRouter
    closeError = metricRegistry.counter(MetricRegistry.name(CoordinatorBackedRouter.class, "CloseError"));
    futureCallbackError =
        metricRegistry.counter(MetricRegistry.name(CoordinatorBackedRouter.class, "FutureCallbackError"));
    unavailableError = metricRegistry.counter(MetricRegistry.name(CoordinatorBackedRouter.class, "UnavailableError"));
    // CoordinatorOperation
    operationError = metricRegistry.counter(MetricRegistry.name(CoordinatorOperation.class, "OperationError"));
    queueStartTimeNotRecordedError =
        metricRegistry.counter(MetricRegistry.name(CoordinatorOperation.class, "QueueStartTimeNotRecordedError"));

    Gauge<Integer> operationsInFlight = new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return totalOperationsInFlight.get();
      }
    };
    metricRegistry
        .register(MetricRegistry.name(CoordinatorBackedRouter.class, "TotalOperationsInFlight"), operationsInFlight);

    Gauge<Integer> operationsInExecution = new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return totalOperationsInExecution.get();
      }
    };
    metricRegistry.register(MetricRegistry.name(CoordinatorBackedRouter.class, "TotalOperationsInExecution"),
        operationsInExecution);
  }
}
