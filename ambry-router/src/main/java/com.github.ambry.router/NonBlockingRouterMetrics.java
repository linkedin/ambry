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
 * {@link NonBlockingRouter} specific metrics tracking.
 * <p/>
 * Exports metrics that are triggered by the {@link NonBlockingRouter} to the provided {@link MetricRegistry}
 */
public class NonBlockingRouterMetrics {
  private final MetricRegistry metricRegistry;
  // @todo: Ensure all metrics here get updated appropriately.
  // @todo: chunk filling rate metrics.
  // @todo: More metrics for the RequestResponse handling (poll, handleResponse etc.)
  // Rates
  public final Meter putBlobOperationRate;
  public final Meter operationQueuingRate;
  public final Meter operationDequeuingRate;

  public final Histogram putBlobOperationLatencyMs;
  public final Histogram putChunkOperationLatencyMs;
  public final Histogram routerRequestLatencyMs;

  // Operation Errors
  public final Counter slippedPutSuccessCount;
  public final Counter putBlobErrorCount;
  public final Counter operationAbortCount;

  // Gauge
  public Gauge<Long> chunkFillerThreadRunning;
  public Gauge<Long> requestResponseHandlerThreadRunning;
  public Gauge<Integer> activeOperations;

  public NonBlockingRouterMetrics(MetricRegistry metricRegistry) {
    this.metricRegistry = metricRegistry;
    putBlobOperationRate = metricRegistry.meter(MetricRegistry.name(PutManager.class, "-PutBlobRequestArrivalRate"));
    operationQueuingRate = metricRegistry.meter(MetricRegistry.name(NonBlockingRouter.class, "-OperationQueuingRate"));
    operationDequeuingRate =
        metricRegistry.meter(MetricRegistry.name(NonBlockingRouter.class, "-OperationDequeuingRate"));

    putBlobOperationLatencyMs =
        metricRegistry.histogram(MetricRegistry.name(PutManager.class, "-PutBlobOperationLatencyMs"));
    putChunkOperationLatencyMs =
        metricRegistry.histogram(MetricRegistry.name(PutManager.class, "-PutChunkOperationLatencyMs"));
    routerRequestLatencyMs =
        metricRegistry.histogram(MetricRegistry.name(NonBlockingRouter.class, "-RouterRequestLatencyMs"));

    slippedPutSuccessCount = metricRegistry.counter(MetricRegistry.name(PutManager.class, "-SlippedPutSuccessCount"));
    putBlobErrorCount = metricRegistry.counter(MetricRegistry.name(PutManager.class, "-PutBlobErrorCount"));
    operationAbortCount = metricRegistry.counter(MetricRegistry.name(NonBlockingRouter.class, "-OperationAbortCount"));
  }

  public void initializeOperationControllerMetrics(final Thread requestResponseHandlerThread) {
    requestResponseHandlerThreadRunning = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return requestResponseHandlerThread.isAlive() ? 1L : 0L;
      }
    };
    metricRegistry
        .register(MetricRegistry.name(NonBlockingRouter.class, requestResponseHandlerThread.getName() + "-Running"),
            requestResponseHandlerThreadRunning);
  }

  public void initializePutManagerMetrics(final Thread chunkFillerThread) {
    chunkFillerThreadRunning = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return chunkFillerThread.isAlive() ? 1L : 0L;
      }
    };
    metricRegistry.register(MetricRegistry.name(NonBlockingRouter.class, chunkFillerThread.getName() + "-Running"),
        chunkFillerThreadRunning);
  }

  public void initializeNumActiveOperationsMetrics(final AtomicInteger currentOperationsCount) {
    activeOperations = new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return currentOperationsCount.get();
      }
    };
    metricRegistry.register(MetricRegistry.name(NonBlockingRouter.class, "-NumActiveOperations"), activeOperations);
  }
}
