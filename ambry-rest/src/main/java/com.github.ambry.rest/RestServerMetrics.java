package com.github.ambry.rest;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link RestServer} and Rest infrastructure ({@link RestRequestHandlerController},
 * {@link RestRequestHandler}) specific metrics tracking.
 * <p/>
 * Exports metrics that are triggered by Rest infrastructure to the provided {@link MetricRegistry}.
 */
class RestServerMetrics {
  private final MetricRegistry metricRegistry;
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final Object asyncRequestHandlerRegisterLock = new Object();

  // Gauges
  // AsyncRequestHandler
  private final List<Gauge<Integer>> asyncRequestHandlerQueueOccupancy;
  private final List<Gauge<Integer>> asyncRequestHandlerRequestsInFlight;

  // Rates
  // AsyncRequestHandler
  public final Meter asyncRequestHandlerQueueingRate;
  public final Meter asyncRequestHandlerRequestArrivalRate;
  // DequeuedRequestHandler
  public final Meter dequeuedRequestHandlerDequeuingRate;
  public final Meter dequeuedRequestHandlerRequestCompletionRate;

  // Latencies

  // Errors
  private final Counter metricAdditionFailure;
  // RequestHandlerController
  public final Counter requestHandlerControllerInstantiationFailure;
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
  public final Counter dequeuedRequestHandlerHandlingCompleteTasksFailure;
  // RestServer
  public final Counter restServerInstantiationFailure;
  public final Counter restServerStartFailure;

  // Others
  // AsyncRequestHandler
  public final Counter asyncRequestHandlerForcedShutdown;
  public final Histogram asyncRequestHandlerQueueTimeInMs;
  public final Histogram asyncRequestHandlerShutdownTimeInMs;
  // RestServer
  public final Histogram restServerStartTimeInMs;
  public final Histogram restServerShutdownTimeInMs;

  public RestServerMetrics(MetricRegistry metricRegistry) {
    this.metricRegistry = metricRegistry;
    asyncRequestHandlerQueueOccupancy = new ArrayList<Gauge<Integer>>();
    asyncRequestHandlerRequestsInFlight = new ArrayList<Gauge<Integer>>();

    asyncRequestHandlerQueueingRate =
        metricRegistry.meter(MetricRegistry.name(AsyncRequestHandler.class, "AsyncRequestHandlerQueueingRate"));
    asyncRequestHandlerRequestArrivalRate =
        metricRegistry.meter(MetricRegistry.name(AsyncRequestHandler.class, "AsyncRequestHandlerRequestArrivalRate"));
    dequeuedRequestHandlerDequeuingRate =
        metricRegistry.meter(MetricRegistry.name(DequeuedRequestHandler.class, "DequeuedRequestHandlerDequeuingRate"));
    dequeuedRequestHandlerRequestCompletionRate = metricRegistry
        .meter(MetricRegistry.name(DequeuedRequestHandler.class, "DequeuedRequestHandlerRequestCompletionRate"));

    metricAdditionFailure = metricRegistry.counter(MetricRegistry.name(getClass(), "MetricAdditionFailure"));
    requestHandlerControllerInstantiationFailure = metricRegistry
        .counter(MetricRegistry.name(RequestHandlerController.class, "RequestHandlerControllerInstantiationFailure"));
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
    asyncRequestHandlerResidualQueueSize =
        metricRegistry.counter(MetricRegistry.name(AsyncRequestHandler.class, "AsyncRequestHandlerResidualQueueSize"));
    dequeuedRequestHandlerUnknownRestMethod = metricRegistry
        .counter(MetricRegistry.name(DequeuedRequestHandler.class, "DequeuedRequestHandlerUnknownRestMethod"));
    dequeuedRequestHandlerRestRequestInfoHandlingFailure = metricRegistry.counter(
        MetricRegistry.name(DequeuedRequestHandler.class, "DequeuedRequestHandlerRestRequestInfoHandlingFailure"));
    dequeuedRequestHandlerQueueTakeInterrupted = metricRegistry
        .counter(MetricRegistry.name(DequeuedRequestHandler.class, "DequeuedRequestHandlerQueueTakeInterrupted"));
    dequeuedRequestHandlerUnexpectedException = metricRegistry
        .counter(MetricRegistry.name(DequeuedRequestHandler.class, "DequeuedRequestHandlerUnexpectedException"));
    dequeuedRequestHandlerHandlingCompleteTasksFailure = metricRegistry.counter(
        MetricRegistry.name(DequeuedRequestHandler.class, "DequeuedRequestHandlerHandlingCompleteTasksFailure"));
    restServerInstantiationFailure =
        metricRegistry.counter(MetricRegistry.name(RestServer.class, "RestServerInstantiationFailure"));
    restServerStartFailure = metricRegistry.counter(MetricRegistry.name(RestServer.class, "RestServerStartFailure"));

    asyncRequestHandlerForcedShutdown =
        metricRegistry.counter(MetricRegistry.name(AsyncRequestHandler.class, "AsyncRequestHandlerForcedShutdown"));
    asyncRequestHandlerQueueTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AsyncRequestHandler.class, "AsyncRequestHandlerQueueTimeInMs"));
    asyncRequestHandlerShutdownTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AsyncRequestHandler.class, "AsyncRequestHandlerShutdownTimeInMs"));
    restServerStartTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(RestServer.class, "RestServerStartTimeInMs"));
    restServerShutdownTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(RestServer.class, "RestServerShutdownTimeInMs"));
  }

  public void registerAsyncRequestHandler(final AsyncRequestHandler requestHandler) {
    synchronized (asyncRequestHandlerRegisterLock) {
      assert asyncRequestHandlerQueueOccupancy.size() == asyncRequestHandlerRequestsInFlight.size();
      int pos = asyncRequestHandlerQueueOccupancy.size();
      Gauge<Integer> gauge = new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          return requestHandler.getQueueSize();
        }
      };
      if (asyncRequestHandlerQueueOccupancy.add(gauge)) {
        metricRegistry
            .register(MetricRegistry.name(AsyncRequestHandler.class, pos + "-AsyncRequestHandlerQueueOccupancy"),
                gauge);
      } else {
        logger.error("Failed to register AsyncRequestHandler to the queue occupancy tracker");
        metricAdditionFailure.inc();
      }

      gauge = new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          return requestHandler.getRequestsInFlightCount();
        }
      };
      if (asyncRequestHandlerRequestsInFlight.add(gauge)) {
        metricRegistry
            .register(MetricRegistry.name(AsyncRequestHandler.class, pos + "-AsyncRequestHandlerRequestsInFlight"),
                gauge);
      } else {
        logger.error("Failed to register AsyncRequestHandler to the requests in flight tracker");
        metricAdditionFailure.inc();
      }
    }
  }
}
