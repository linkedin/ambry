package com.github.ambry.rest;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;


/**
 * {@link RestServer} and Rest infrastructure ({@link RestRequestHandlerController},
 * {@link RestRequestHandler}) specific metrics tracking.
 * <p/>
 * Exports metrics that are triggered by Rest infrastructure to the provided {@link MetricRegistry}.
 */
class RestServerMetrics {
  // Rates
  // AsyncRequestHandler
  public final Meter asyncRequestHandlerQueueingRate;
  // DequeuedRequestHandler
  public final Meter dequeuedRequestHandlerDequeuingRate;

  // Latencies

  // Errors
  // RequestHandlerController
  public final Counter requestHandlerControllerHandlerSelectionError;
  // AsyncRequestHandler
  public final Counter asyncRequestHandlerQueueOfferTooLong;
  public final Counter asyncRequestHandlerQueueOfferInterrupted;
  public final Counter asyncRequestHandlerQueueTakeInterrupted;
  public final Counter asyncRequestHandlerRestRequestInfoNull;
  public final Counter asyncRequestHandlerRestResponseHandlerNull;
  public final Counter asyncRequestHandlerRestRequestMetadataNull;
  public final Counter asyncRequestHandlerUnavailable;
  public final Counter asyncRequestHandlerRequestAlreadyInFlight;
  // DequeuedRequestHandler
  public final Counter dequeuedRequestHandlerUnknownRestMethod;
  public final Counter dequeuedRequestHandlerRestRequestInfoHandlingFailure;

  // Others
  public final Histogram asyncRequestHandlerQueueOccupancy;

  public RestServerMetrics(MetricRegistry metricRegistry) {
    asyncRequestHandlerQueueingRate =
        metricRegistry.meter(MetricRegistry.name(AsyncRequestHandler.class, "AsyncRequestHandlerQueueingRate"));
    dequeuedRequestHandlerDequeuingRate =
        metricRegistry.meter(MetricRegistry.name(DequeuedRequestHandler.class, "DequeuedRequestHandlerDequeuingRate"));

    requestHandlerControllerHandlerSelectionError = metricRegistry
        .counter(MetricRegistry.name(RequestHandlerController.class, "RequestHandlerControllerHandlerSelectionError"));
    asyncRequestHandlerQueueOfferTooLong =
        metricRegistry.counter(MetricRegistry.name(AsyncRequestHandler.class, "AsyncRequestHandlerQueueOfferTooLong"));
    asyncRequestHandlerQueueOfferInterrupted = metricRegistry
        .counter(MetricRegistry.name(AsyncRequestHandler.class, "AsyncRequestHandlerQueueOfferInterrupted"));
    asyncRequestHandlerQueueTakeInterrupted = metricRegistry
        .counter(MetricRegistry.name(AsyncRequestHandler.class, "AsyncRequestHandlerQueueTakeInterrupted"));
    asyncRequestHandlerRestRequestInfoNull = metricRegistry
        .counter(MetricRegistry.name(AsyncRequestHandler.class, "AsyncRequestHandlerRestRequestInfoNull"));
    asyncRequestHandlerRestResponseHandlerNull = metricRegistry
        .counter(MetricRegistry.name(AsyncRequestHandler.class, "AsyncRequestHandlerResponseHandlerNull"));
    asyncRequestHandlerRestRequestMetadataNull = metricRegistry
        .counter(MetricRegistry.name(AsyncRequestHandler.class, "AsyncRequestHandlerRestRequestMetadataNull"));
    asyncRequestHandlerUnavailable = metricRegistry
        .counter(MetricRegistry.name(AsyncRequestHandler.class, "AsyncRequestHandlerUnavailable"));
    asyncRequestHandlerRequestAlreadyInFlight = metricRegistry
        .counter(MetricRegistry.name(AsyncRequestHandler.class, "AsyncRequestHandlerRequestAlreadyInFlight"));
    dequeuedRequestHandlerUnknownRestMethod = metricRegistry
        .counter(MetricRegistry.name(DequeuedRequestHandler.class, "DequeuedRequestHandlerUnknownRestMethod"));
    dequeuedRequestHandlerRestRequestInfoHandlingFailure = metricRegistry.counter(
        MetricRegistry.name(DequeuedRequestHandler.class, "DequeuedRequestHandlerRestRequestInfoHandlingFailure"));

    asyncRequestHandlerQueueOccupancy =
        metricRegistry.histogram(MetricRegistry.name(AsyncRequestHandler.class, "AsyncRequestHandlerQueueOccupancy"));
  }
}
