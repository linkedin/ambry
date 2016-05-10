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
package com.github.ambry.rest;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * {@link RestServer} and Rest infrastructure ({@link AsyncRequestResponseHandler}) specific metrics tracking.
 * <p/>
 * Exports metrics that are triggered by Rest infrastructure to the provided {@link MetricRegistry}.
 */
class RestServerMetrics {
  private final MetricRegistry metricRegistry;
  private final AtomicInteger asyncRequestWorkerIndex = new AtomicInteger(0);

  // Rates
  // AsyncRequestWorker
  public final Meter requestArrivalRate;
  public final Meter requestDequeuingRate;
  public final Meter requestQueuingRate;
  // AsyncResponseHandler
  public final Meter responseArrivalRate;
  public final Meter responseCompletionRate;

  // Latencies
  // AsyncRequestWorker
  public final Histogram requestPreProcessingTimeInMs;
  // AsyncResponseHandler
  public final Histogram responseCallbackProcessingTimeInMs;
  public final Histogram responseCallbackWaitTimeInMs;
  public final Histogram responsePreProcessingTimeInMs;
  // AsyncRequestResponseHandler
  public final Histogram requestWorkerSelectionTimeInMs;

  // Errors
  // AsyncRequestWorker
  public final Counter requestProcessingError;
  public final Counter requestQueueAddError;
  public final Counter unknownRestMethodError;
  // AsyncResponseHandler
  public final Counter resourceReleaseError;
  public final Counter responseAlreadyInFlightError;
  public final Counter responseCompleteTasksError;
  // AsyncRequestResponseHandler
  public final Counter requestResponseHandlerShutdownError;
  public final Counter requestResponseHandlerUnavailableError;
  // RestServer
  public final Counter restServerInstantiationError;

  // Others
  // AsyncResponseHandler
  public final Counter responseExceptionCount;
  public final Histogram responseHandlerCloseTimeInMs;
  // AsyncRequestResponseHandler
  public final Histogram requestWorkerShutdownTimeInMs;
  public final Histogram requestWorkerStartTimeInMs;
  public final Histogram requestResponseHandlerShutdownTimeInMs;
  public final Histogram requestResponseHandlerStartTimeInMs;
  public final Counter residualRequestQueueSize;
  public final Counter residualResponseSetSize;
  // RestServer
  public final Histogram blobStorageServiceShutdownTimeInMs;
  public final Histogram blobStorageServiceStartTimeInMs;
  public final Histogram nioServerShutdownTimeInMs;
  public final Histogram nioServerStartTimeInMs;
  public final Histogram jmxReporterShutdownTimeInMs;
  public final Histogram jmxReporterStartTimeInMs;
  public final Histogram restRequestHandlerShutdownTimeInMs;
  public final Histogram restRequestHandlerStartTimeInMs;
  public final Histogram restResponseHandlerShutdownTimeInMs;
  public final Histogram restResponseHandlerStartTimeInMs;
  public final Histogram restServerShutdownTimeInMs;
  public final Histogram restServerStartTimeInMs;
  public final Histogram routerCloseTime;

  /**
   * Creates an instance of RestServerMetrics using the given {@code metricRegistry}.
   * @param metricRegistry the {@link MetricRegistry} to use for the metrics.
   */
  public RestServerMetrics(MetricRegistry metricRegistry, final RestServerState restServerState) {
    this.metricRegistry = metricRegistry;

    // Rates
    // AsyncRequestWorker
    requestArrivalRate = metricRegistry.meter(MetricRegistry.name(AsyncRequestWorker.class, "RequestArrivalRate"));
    requestDequeuingRate = metricRegistry.meter(MetricRegistry.name(AsyncRequestWorker.class, "RequestDequeuingRate"));
    requestQueuingRate = metricRegistry.meter(MetricRegistry.name(AsyncRequestWorker.class, "RequestQueuingRate"));
    // AsyncResponseHandler
    responseArrivalRate = metricRegistry.meter(MetricRegistry.name(AsyncResponseHandler.class, "ResponseArrivalRate"));
    responseCompletionRate =
        metricRegistry.meter(MetricRegistry.name(AsyncResponseHandler.class, "ResponseCompletionRate"));

    // Latencies
    // AsyncRequestWorker
    requestPreProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AsyncRequestWorker.class, "RequestPreProcessingTimeInMs"));
    // AsyncResponseHandler
    responseCallbackProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AsyncResponseHandler.class, "ResponseCallbackProcessingTimeInMs"));
    responseCallbackWaitTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AsyncResponseHandler.class, "ResponseCallbackWaitTimeInMs"));
    responsePreProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AsyncResponseHandler.class, "ResponsePreProcessingTimeInMs"));
    // AsyncRequestResponseHandler
    requestWorkerSelectionTimeInMs = metricRegistry
        .histogram(MetricRegistry.name(AsyncRequestResponseHandler.class, "RequestWorkerSelectionTimeInMs"));

    // Errors
    // AsyncRequestWorker
    requestProcessingError =
        metricRegistry.counter(MetricRegistry.name(AsyncRequestWorker.class, "RequestProcessingError"));
    requestQueueAddError =
        metricRegistry.counter(MetricRegistry.name(AsyncRequestWorker.class, "RequestQueueAddError"));
    unknownRestMethodError =
        metricRegistry.counter(MetricRegistry.name(AsyncRequestWorker.class, "UnknownRestMethodError"));
    // AsyncResponseHandler
    resourceReleaseError =
        metricRegistry.counter(MetricRegistry.name(AsyncResponseHandler.class, "ResourceReleaseError"));
    responseAlreadyInFlightError =
        metricRegistry.counter(MetricRegistry.name(AsyncResponseHandler.class, "ResponseAlreadyInFlightError"));
    responseCompleteTasksError =
        metricRegistry.counter(MetricRegistry.name(AsyncResponseHandler.class, "ResponseCompleteTasksError"));
    // AsyncRequestResponseHandler
    requestResponseHandlerShutdownError =
        metricRegistry.counter(MetricRegistry.name(AsyncRequestResponseHandler.class, "ShutdownError"));
    requestResponseHandlerUnavailableError =
        metricRegistry.counter(MetricRegistry.name(AsyncRequestResponseHandler.class, "UnavailableError"));
    // RestServer
    restServerInstantiationError = metricRegistry.counter(MetricRegistry.name(RestServer.class, "InstantiationError"));

    // Others
    // AsyncResponseHandler
    responseExceptionCount =
        metricRegistry.counter(MetricRegistry.name(AsyncResponseHandler.class, "ResponseExceptionCount"));
    responseHandlerCloseTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AsyncResponseHandler.class, "CloseTimeInMs"));
    // AsyncRequestResponseHandler
    requestWorkerShutdownTimeInMs = metricRegistry
        .histogram(MetricRegistry.name(AsyncRequestResponseHandler.class, "RequestWorkerShutdownTimeInMs"));
    requestWorkerStartTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AsyncRequestResponseHandler.class, "RequestWorkerStartTimeInMs"));
    requestResponseHandlerShutdownTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AsyncRequestResponseHandler.class, "ShutdownTimeInMs"));
    requestResponseHandlerStartTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AsyncRequestResponseHandler.class, "StartTimeInMs"));
    // AsyncRequestWorker
    residualRequestQueueSize =
        metricRegistry.counter(MetricRegistry.name(AsyncRequestWorker.class, "ResidualRequestQueueSize"));
    residualResponseSetSize =
        metricRegistry.counter(MetricRegistry.name(AsyncRequestWorker.class, "ResidualResponseSetSize"));
    // RestServer
    blobStorageServiceShutdownTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(RestServer.class, "BlobStorageServiceShutdownTimeInMs"));
    blobStorageServiceStartTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(RestServer.class, "BlobStorageServiceStartTimeInMs"));
    jmxReporterShutdownTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(RestServer.class, "JmxShutdownTimeInMs"));
    jmxReporterStartTimeInMs = metricRegistry.histogram(MetricRegistry.name(RestServer.class, "JmxStartTimeInMs"));
    nioServerShutdownTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(RestServer.class, "NioServerShutdownTimeInMs"));
    nioServerStartTimeInMs = metricRegistry.histogram(MetricRegistry.name(RestServer.class, "NioServerStartTimeInMs"));
    restRequestHandlerShutdownTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(RestServer.class, "RestRequestHandlerShutdownTimeInMs"));
    restRequestHandlerStartTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(RestServer.class, "RestRequestHandlerStartTimeInMs"));
    restResponseHandlerShutdownTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(RestServer.class, "RestResponseHandlerShutdownTimeInMs"));
    restResponseHandlerStartTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(RestServer.class, "RestResponseHandlerStartTimeInMs"));
    restServerShutdownTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(RestServer.class, "RestServerShutdownTimeInMs"));
    restServerStartTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(RestServer.class, "RestServerStartTimeInMs"));
    routerCloseTime = metricRegistry.histogram(MetricRegistry.name(RestServer.class, "RouterCloseTimeInMs"));

    Gauge<Integer> restServerStatus = new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return restServerState.isServiceUp() ? 1 : 0;
      }
    };
    metricRegistry.register(MetricRegistry.name(RestServer.class, "RestServerState"), restServerStatus);
  }

  /**
   * Registers the {@code asyncRequestWorker} so that its metrics (request queue size) can be tracked.
   * @param asyncRequestWorker the {@link AsyncRequestWorker} whose metrics need to be tracked.
   */
  public void registerRequestWorker(final AsyncRequestWorker asyncRequestWorker) {
    int pos = asyncRequestWorkerIndex.getAndIncrement();
    Gauge<Integer> gauge = new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return asyncRequestWorker.getRequestQueueSize();
      }
    };
    metricRegistry.register(MetricRegistry.name(AsyncRequestWorker.class, pos + "-RequestQueueSize"), gauge);
  }

  /**
   * Periodically reports key metrics of the {@code asyncRequestResponseHandler}.
   * @param asyncRequestResponseHandler the {@link AsyncRequestResponseHandler} whose key metrics have to be tracked.
   */
  public void trackAsyncRequestResponseHandler(final AsyncRequestResponseHandler asyncRequestResponseHandler) {
    Gauge<Integer> totalRequestQueueSize = new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return asyncRequestResponseHandler.getRequestQueueSize();
      }
    };
    metricRegistry.register(MetricRegistry.name(AsyncRequestResponseHandler.class, "TotalRequestQueueSize"),
        totalRequestQueueSize);

    Gauge<Integer> totalResponseSetSize = new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return asyncRequestResponseHandler.getResponseSetSize();
      }
    };
    metricRegistry
        .register(MetricRegistry.name(AsyncRequestResponseHandler.class, "TotalResponseSetSize"), totalResponseSetSize);

    Gauge<Integer> asyncHandlerWorkersAlive = new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return asyncRequestResponseHandler.getWorkersAlive();
      }
    };
    metricRegistry.register(MetricRegistry.name(AsyncRequestResponseHandler.class, "AsyncHandlerWorkersAlive"),
        asyncHandlerWorkersAlive);
  }
}
