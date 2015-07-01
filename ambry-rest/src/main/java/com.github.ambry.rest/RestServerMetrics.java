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
  public final Meter asyncRequestHandlerRequestArrivalRate;
  // DequeuedRequestHandler
  public final Meter dequeuedRequestHandlerDequeuingRate;
  public final Meter dequeuedRequestHandlerRequestCompletionRate;

  // Latencies

  // Errors
  // RequestHandlerController
  public final Counter requestHandlerControllerHandlerSelectionError;
  // AsyncRequestHandler
  public final Counter asyncRequestHandlerQueueOfferTooLong;
  public final Counter asyncRequestHandlerQueueOfferInterrupted;
  public final Counter asyncRequestHandlerRestRequestInfoNull;
  public final Counter asyncRequestHandlerRestResponseHandlerNull;
  public final Counter asyncRequestHandlerRestRequestMetadataNull;
  public final Counter asyncRequestHandlerUnavailable;
  public final Counter asyncRequestHandlerRequestAlreadyInFlight;
  public final Counter asyncRequestHandlerShutdownFailure;
  public final Counter asyncRequestHandlerResidualQueueSize;
  // DequeuedRequestHandler
  public final Counter dequeuedRequestHandlerUnknownRestMethod;
  public final Counter dequeuedRequestHandlerRestRequestInfoHandlingFailure;
  public final Counter dequeuedRequestHandlerQueueTakeInterrupted;
  public final Counter dequeuedRequestHandlerUnexpectedException;
  public final Counter dequeuedRequestHandlerCompleteRequestNotMarkedInFlight;
  public final Counter dequeuedRequestHandlerHandlingCompleteTasksFailure;
  public final Counter restServerStartFailure;

  // Others
  public final Counter asyncRequestHandlerForcedShutdown;
  public final Histogram asyncRequestHandlerQueueOccupancy;
  public final Histogram asyncRequestHandlerQueueTimeInMs;
  public final Histogram asyncRequestHandlerRequestsInFlight;
  public final Histogram asyncRequestHandlerShutdownTimeInMs;
  public final Histogram restServerStartTimeInMs;
  public final Histogram restServerShutdownTimeInMs;

  public RestServerMetrics(MetricRegistry metricRegistry) {
    asyncRequestHandlerQueueingRate =
        metricRegistry.meter(MetricRegistry.name(AsyncRequestHandler.class, "AsyncRequestHandlerQueueingRate"));
    asyncRequestHandlerRequestArrivalRate =
        metricRegistry.meter(MetricRegistry.name(AsyncRequestHandler.class, "AsyncRequestHandlerRequestArrivalRate"));
    dequeuedRequestHandlerDequeuingRate =
        metricRegistry.meter(MetricRegistry.name(DequeuedRequestHandler.class, "DequeuedRequestHandlerDequeuingRate"));
    dequeuedRequestHandlerRequestCompletionRate = metricRegistry
        .meter(MetricRegistry.name(DequeuedRequestHandler.class, "DequeuedRequestHandlerRequestCompletionRate"));

    requestHandlerControllerHandlerSelectionError = metricRegistry
        .counter(MetricRegistry.name(RequestHandlerController.class, "RequestHandlerControllerHandlerSelectionError"));
    asyncRequestHandlerQueueOfferTooLong =
        metricRegistry.counter(MetricRegistry.name(AsyncRequestHandler.class, "AsyncRequestHandlerQueueOfferTooLong"));
    asyncRequestHandlerQueueOfferInterrupted = metricRegistry
        .counter(MetricRegistry.name(AsyncRequestHandler.class, "AsyncRequestHandlerQueueOfferInterrupted"));
    asyncRequestHandlerRestRequestInfoNull = metricRegistry
        .counter(MetricRegistry.name(AsyncRequestHandler.class, "AsyncRequestHandlerRestRequestInfoNull"));
    asyncRequestHandlerRestResponseHandlerNull = metricRegistry
        .counter(MetricRegistry.name(AsyncRequestHandler.class, "AsyncRequestHandlerResponseHandlerNull"));
    asyncRequestHandlerRestRequestMetadataNull = metricRegistry
        .counter(MetricRegistry.name(AsyncRequestHandler.class, "AsyncRequestHandlerRestRequestMetadataNull"));
    asyncRequestHandlerUnavailable =
        metricRegistry.counter(MetricRegistry.name(AsyncRequestHandler.class, "AsyncRequestHandlerUnavailable"));
    asyncRequestHandlerRequestAlreadyInFlight = metricRegistry
        .counter(MetricRegistry.name(AsyncRequestHandler.class, "AsyncRequestHandlerRequestAlreadyInFlight"));
    asyncRequestHandlerShutdownFailure =
        metricRegistry.counter(MetricRegistry.name(AsyncRequestHandler.class, "AsyncRequestHandlerShutdownFailure"));
    asyncRequestHandlerResidualQueueSize = metricRegistry
        .counter(MetricRegistry.name(AsyncRequestHandler.class, "AsyncRequestHandlerResidualQueueSize"));
    dequeuedRequestHandlerUnknownRestMethod = metricRegistry
        .counter(MetricRegistry.name(DequeuedRequestHandler.class, "DequeuedRequestHandlerUnknownRestMethod"));
    dequeuedRequestHandlerRestRequestInfoHandlingFailure = metricRegistry.counter(
        MetricRegistry.name(DequeuedRequestHandler.class, "DequeuedRequestHandlerRestRequestInfoHandlingFailure"));
    dequeuedRequestHandlerQueueTakeInterrupted = metricRegistry
        .counter(MetricRegistry.name(DequeuedRequestHandler.class, "DequeuedRequestHandlerQueueTakeInterrupted"));
    dequeuedRequestHandlerUnexpectedException = metricRegistry
        .counter(MetricRegistry.name(DequeuedRequestHandler.class, "DequeuedRequestHandlerUnexpectedException"));
    dequeuedRequestHandlerCompleteRequestNotMarkedInFlight = metricRegistry.counter(
        MetricRegistry.name(DequeuedRequestHandler.class, "DequeuedRequestHandlerCompleteRequestNotMarkedInFlight"));
    dequeuedRequestHandlerHandlingCompleteTasksFailure = metricRegistry.counter(
        MetricRegistry.name(DequeuedRequestHandler.class, "DequeuedRequestHandlerHandlingCompleteTasksFailure"));
    restServerStartFailure = metricRegistry.counter(MetricRegistry.name(RestServer.class, "RestServerStartFailure"));

    asyncRequestHandlerForcedShutdown =
        metricRegistry.counter(MetricRegistry.name(AsyncRequestHandler.class, "AsyncRequestHandlerForcedShutdown"));
    asyncRequestHandlerQueueOccupancy =
        metricRegistry.histogram(MetricRegistry.name(AsyncRequestHandler.class, "AsyncRequestHandlerQueueOccupancy"));
    asyncRequestHandlerQueueTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AsyncRequestHandler.class, "AsyncRequestHandlerQueueTimeInMs"));
    asyncRequestHandlerRequestsInFlight =
        metricRegistry.histogram(MetricRegistry.name(AsyncRequestHandler.class, "AsyncRequestHandlerRequestsInFlight"));
    asyncRequestHandlerShutdownTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AsyncRequestHandler.class, "AsyncRequestHandlerShutdownTimeInMs"));
    restServerStartTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(RestServer.class, "RestServerStartTimeInMs"));
    restServerShutdownTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(RestServer.class, "RestServerShutdownTimeInMs"));
  }
}
