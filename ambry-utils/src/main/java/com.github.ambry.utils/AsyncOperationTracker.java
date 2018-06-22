/*
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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
 *
 */
package com.github.ambry.utils;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;


/**
 * Tracks metrics and logs progress of asynchronous operations that accept callbacks.
 */
public class AsyncOperationTracker {
  private long operationStartTime = 0;
  private long processingStartTime = 0;

  private final String context;
  private final Logger logger;
  private final Metrics metrics;

  /**
   * Create a AsyncOperationTracker that tracks a particular operation.
   * @param context Description of the callback context that is included in the trace logging messages.
   * @param metrics the {@link Metrics}
   */
  public AsyncOperationTracker(String context, Logger logger, Metrics metrics) {
    this.logger = logger;
    this.metrics = metrics;
    this.context = context;
  }

  /**
   * Marks that the operation being tracked has started.
   */
  public void markOperationStart() {
    logger.trace("{} started for {}", metrics.operationType, context);
    operationStartTime = System.currentTimeMillis();
  }

  /**
   * Marks that the operation being tracked has ended and callback processing has started.
   */
  public void markOperationEnd() {
    logger.trace("{} finished for {}", metrics.operationType, context);
    processingStartTime = System.currentTimeMillis();
    long operationTime = processingStartTime - operationStartTime;
    metrics.operationTimeInMs.update(operationTime);
  }

  /**
   * Marks that the  callback processing has ended.
   */
  public void markCallbackProcessingEnd() {
    logger.trace("Callback for {} of {} finished", metrics.operationType, context);
    long processingTime = System.currentTimeMillis() - processingStartTime;
    metrics.callbackProcessingTimeInMs.update(processingTime);
  }

  /**
   * Mark if an exception occurred within the callback.
   */
  public void markCallbackProcessingError() {
    logger.trace("Exception thrown in callback for {} of {}", metrics.operationType, context);
    metrics.callbackProcessingError.inc();
  }

  /**
   * Metrics that track the progress of an async operation (including processing done in its callback).
   */
  public static class Metrics {
    private final String operationType;
    private final Histogram operationTimeInMs;
    private final Histogram callbackProcessingTimeInMs;
    private final Counter callbackProcessingError;

    /**
     * Add the async operation tracking metrics to a metric registry.
     * @param ownerClass the owning class for the metrics.
     * @param operationType the type of async operation.
     * @param metricRegistry the {@link MetricRegistry} to register the metrics in.
     */
    public Metrics(Class ownerClass, String operationType, MetricRegistry metricRegistry) {
      this.operationType = operationType;
      this.operationTimeInMs = metricRegistry.histogram(MetricRegistry.name(ownerClass, operationType + "TimeInMs"));
      this.callbackProcessingTimeInMs =
          metricRegistry.histogram(MetricRegistry.name(ownerClass, operationType + "CallbackProcessingTimeInMs"));
      this.callbackProcessingError = metricRegistry.counter(MetricRegistry.name(ownerClass, "CallbackProcessingError"));
    }
  }
}
