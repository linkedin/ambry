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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link AsyncRequestResponseHandler} specific implementation of {@link RestRequestResponseHandlerFactory}.
 * <p/>
 * Sets up all the supporting cast required for {@link AsyncRequestResponseHandler}. Maintains a single handler of
 * {@link AsyncRequestResponseHandler} and returns the same handler on any call to {@link #getRestRequestHandler()} or
 * {@link #getRestResponseHandler()}.
 */
public class AsyncRequestResponseHandlerFactory implements RestRequestResponseHandlerFactory {

  private final AsyncRequestResponseHandler handler;
  private Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * @param handlerCount the number of request scaling units required.
   * @param metricRegistry the {@link MetricRegistry} handler that should be used for metrics.
   * @param restRequestService the {@link RestRequestService} to use for handling requests.
   * @throws IllegalArgumentException if {@code handlerCount} <= 0 or if {@code metricRegistry} or
   * {@code restRequestService} is null.
   */
  public AsyncRequestResponseHandlerFactory(Integer handlerCount, MetricRegistry metricRegistry,
      RestRequestService restRequestService) {
    if (metricRegistry == null || restRequestService == null) {
      throw new IllegalArgumentException("One or more arguments received is null");
    } else if (handlerCount <= 0) {
      throw new IllegalArgumentException("Request handler scaling unit count has to be > 0. Is " + handlerCount);
    } else {
      handler = new AsyncRequestResponseHandler(new RequestResponseHandlerMetrics(metricRegistry), handlerCount,
          restRequestService);
    }
    logger.trace("Instantiated AsyncRequestResponseHandlerFactory as RestRequestHandler");
  }

  /**
   * @return {@link AsyncRequestResponseHandler}.
   */
  @Override
  public RestRequestHandler getRestRequestHandler() {
    return handler;
  }

  /**
   * @return {@link AsyncRequestResponseHandler}.
   */
  @Override
  public RestResponseHandler getRestResponseHandler() {
    return handler;
  }
}

/**
 * {@link AsyncRequestResponseHandler} specific metrics tracking.
 */
class RequestResponseHandlerMetrics {
  final MetricRegistry metricRegistry;
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

  /**
   * Creates an instance of RequestResponseHandlerMetrics using the given {@code metricRegistry}.
   * @param metricRegistry the {@link MetricRegistry} to use for the metrics.
   */
  public RequestResponseHandlerMetrics(MetricRegistry metricRegistry) {
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
    requestWorkerSelectionTimeInMs = metricRegistry.histogram(
        MetricRegistry.name(AsyncRequestResponseHandler.class, "RequestWorkerSelectionTimeInMs"));

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

    // Others
    // AsyncResponseHandler
    responseExceptionCount =
        metricRegistry.counter(MetricRegistry.name(AsyncResponseHandler.class, "ResponseExceptionCount"));
    responseHandlerCloseTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AsyncResponseHandler.class, "CloseTimeInMs"));
    // AsyncRequestResponseHandler
    requestWorkerShutdownTimeInMs = metricRegistry.histogram(
        MetricRegistry.name(AsyncRequestResponseHandler.class, "RequestWorkerShutdownTimeInMs"));
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
  }

  /**
   * Registers the {@code asyncRequestWorker} so that its metrics (request queue size) can be tracked.
   * @param asyncRequestWorker the {@link AsyncRequestWorker} whose metrics need to be tracked.
   */
  public void registerRequestWorker(final AsyncRequestWorker asyncRequestWorker) {
    int pos = asyncRequestWorkerIndex.getAndIncrement();
    Gauge<Integer> gauge = asyncRequestWorker::getRequestQueueSize;
    metricRegistry.register(MetricRegistry.name(AsyncRequestWorker.class, pos + "-RequestQueueSize"), gauge);
  }

  /**
   * Periodically reports key metrics of the {@code asyncRequestResponseHandler}.
   * @param asyncRequestResponseHandler the {@link AsyncRequestResponseHandler} whose key metrics have to be tracked.
   */
  public void trackAsyncRequestResponseHandler(final AsyncRequestResponseHandler asyncRequestResponseHandler) {
    Gauge<Integer> totalRequestQueueSize = asyncRequestResponseHandler::getRequestQueueSize;
    metricRegistry.register(MetricRegistry.name(AsyncRequestResponseHandler.class, "TotalRequestQueueSize"),
        totalRequestQueueSize);

    Gauge<Integer> totalResponseSetSize = asyncRequestResponseHandler::getResponseSetSize;
    metricRegistry.register(MetricRegistry.name(AsyncRequestResponseHandler.class, "TotalResponseSetSize"),
        totalResponseSetSize);

    Gauge<Integer> asyncHandlerWorkersAlive = asyncRequestResponseHandler::getWorkersAlive;
    metricRegistry.register(MetricRegistry.name(AsyncRequestResponseHandler.class, "AsyncHandlerWorkersAlive"),
        asyncHandlerWorkersAlive);
  }
}
