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
  private final List<Gauge<Integer>> asyncRequestHandlerQueueOccupancyGauges;
  private final List<Gauge<Integer>> asyncRequestHandlerRequestsInFlightGauges;
  private Gauge<Integer> requestHandlersAlive;

  // Rates
  // AsyncRequestHandler
  public final Meter asyncRequestHandlerQueueingRate;
  public final Meter asyncRequestHandlerRequestArrivalRate;
  // DequeuedRequestHandler
  public final Meter dequeuedRequestHandlerDequeuingRate;
  public final Meter dequeuedRequestHandlerRequestCompletionRate;

  // Latencies

  // Errors
  private final Counter metricAdditionError;
  // RequestHandlerController
  public final Counter requestHandlerControllerInstantiationError;
  public final Counter requestHandlerSelectionError;
  // AsyncRequestHandler
  public final Counter asyncRequestHandlerQueueOfferTooLongError;
  public final Counter asyncRequestHandlerQueueOfferInterruptedError;
  public final Counter asyncRequestHandlerRestRequestInfoNullError;
  public final Counter asyncRequestHandlerRestResponseHandlerNullError;
  public final Counter asyncRequestHandlerRestRequestMetadataNullError;
  public final Counter asyncRequestHandlerUnavailableError;
  public final Counter asyncRequestHandlerRequestAlreadyInFlightError;
  public final Counter asyncRequestHandlerShutdownError;
  public final Counter asyncRequestHandlerResidualQueueSize;
  // DequeuedRequestHandler
  public final Counter dequeuedRequestHandlerUnknownRestMethodError;
  public final Counter dequeuedRequestHandlerRestRequestInfoHandlingError;
  public final Counter dequeuedRequestHandlerQueueTakeInterruptedError;
  public final Counter dequeuedRequestHandlerUnexpectedExceptionError;
  public final Counter dequeuedRequestHandlerHandlingCompleteTasksError;
  // RestServer
  public final Counter restServerInstantiationError;

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
    asyncRequestHandlerQueueOccupancyGauges = new ArrayList<Gauge<Integer>>();
    asyncRequestHandlerRequestsInFlightGauges = new ArrayList<Gauge<Integer>>();

    asyncRequestHandlerQueueingRate =
        metricRegistry.meter(MetricRegistry.name(AsyncRequestHandler.class, "QueueingRate"));
    asyncRequestHandlerRequestArrivalRate =
        metricRegistry.meter(MetricRegistry.name(AsyncRequestHandler.class, "RequestArrivalRate"));
    dequeuedRequestHandlerDequeuingRate =
        metricRegistry.meter(MetricRegistry.name(DequeuedRequestHandler.class, "DequeuingRate"));
    dequeuedRequestHandlerRequestCompletionRate =
        metricRegistry.meter(MetricRegistry.name(DequeuedRequestHandler.class, "RequestCompletionRate"));

    metricAdditionError = metricRegistry.counter(MetricRegistry.name(getClass(), "MetricAdditionError"));
    requestHandlerControllerInstantiationError =
        metricRegistry.counter(MetricRegistry.name(RequestHandlerController.class, "InstantiationError"));
    requestHandlerSelectionError =
        metricRegistry.counter(MetricRegistry.name(RequestHandlerController.class, "RequestHandlerSelectionError"));
    asyncRequestHandlerQueueOfferTooLongError =
        metricRegistry.counter(MetricRegistry.name(AsyncRequestHandler.class, "QueueOfferTooLongError"));
    asyncRequestHandlerQueueOfferInterruptedError =
        metricRegistry.counter(MetricRegistry.name(AsyncRequestHandler.class, "QueueOfferInterruptedError"));
    asyncRequestHandlerRestRequestInfoNullError =
        metricRegistry.counter(MetricRegistry.name(AsyncRequestHandler.class, "RestRequestInfoNullError"));
    asyncRequestHandlerRestResponseHandlerNullError =
        metricRegistry.counter(MetricRegistry.name(AsyncRequestHandler.class, "ResponseHandlerNullError"));
    asyncRequestHandlerRestRequestMetadataNullError =
        metricRegistry.counter(MetricRegistry.name(AsyncRequestHandler.class, "RestRequestMetadataNullError"));
    asyncRequestHandlerUnavailableError =
        metricRegistry.counter(MetricRegistry.name(AsyncRequestHandler.class, "UnavailableError"));
    asyncRequestHandlerRequestAlreadyInFlightError =
        metricRegistry.counter(MetricRegistry.name(AsyncRequestHandler.class, "RequestAlreadyInFlightError"));
    asyncRequestHandlerShutdownError =
        metricRegistry.counter(MetricRegistry.name(AsyncRequestHandler.class, "ShutdownError"));
    asyncRequestHandlerResidualQueueSize =
        metricRegistry.counter(MetricRegistry.name(AsyncRequestHandler.class, "ResidualQueueSize"));
    dequeuedRequestHandlerUnknownRestMethodError =
        metricRegistry.counter(MetricRegistry.name(DequeuedRequestHandler.class, "UnknownRestMethodError"));
    dequeuedRequestHandlerRestRequestInfoHandlingError =
        metricRegistry.counter(MetricRegistry.name(DequeuedRequestHandler.class, "RestRequestInfoHandlingError"));
    dequeuedRequestHandlerQueueTakeInterruptedError =
        metricRegistry.counter(MetricRegistry.name(DequeuedRequestHandler.class, "QueueTakeInterruptedError"));
    dequeuedRequestHandlerUnexpectedExceptionError =
        metricRegistry.counter(MetricRegistry.name(DequeuedRequestHandler.class, "UnexpectedExceptionError"));
    dequeuedRequestHandlerHandlingCompleteTasksError =
        metricRegistry.counter(MetricRegistry.name(DequeuedRequestHandler.class, "OnHandlingCompleteTasksError"));
    restServerInstantiationError = metricRegistry.counter(MetricRegistry.name(RestServer.class, "InstantiationError"));

    asyncRequestHandlerForcedShutdown =
        metricRegistry.counter(MetricRegistry.name(AsyncRequestHandler.class, "ForcedShutdown"));
    asyncRequestHandlerQueueTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AsyncRequestHandler.class, "QueueTimeInMs"));
    asyncRequestHandlerShutdownTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AsyncRequestHandler.class, "ShutdownTimeInMs"));
    restServerStartTimeInMs = metricRegistry.histogram(MetricRegistry.name(RestServer.class, "StartTimeInMs"));
    restServerShutdownTimeInMs = metricRegistry.histogram(MetricRegistry.name(RestServer.class, "ShutdownTimeInMs"));
  }

  /**
   * Registers an {@link AsyncRequestHandler} so that its metrics (queue occupancy, requests in flight) can be tracked.
   * @param requestHandler the {@link AsyncRequestHandler} whose metrics need to be tracked.
   */
  public void registerAsyncRequestHandler(final AsyncRequestHandler requestHandler) {
    synchronized (asyncRequestHandlerRegisterLock) {
      assert asyncRequestHandlerQueueOccupancyGauges.size() == asyncRequestHandlerRequestsInFlightGauges.size();
      int pos = asyncRequestHandlerQueueOccupancyGauges.size();
      Gauge<Integer> gauge = new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          return requestHandler.getQueueSize();
        }
      };
      if (asyncRequestHandlerQueueOccupancyGauges.add(gauge)) {
        metricRegistry.register(MetricRegistry.name(AsyncRequestHandler.class, pos + "-QueueOccupancy"), gauge);
      } else {
        logger.warn("Failed to register AsyncRequestHandler to the queue occupancy tracker");
        metricAdditionError.inc();
      }

      gauge = new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          return requestHandler.getRequestsInFlightCount();
        }
      };
      if (asyncRequestHandlerRequestsInFlightGauges.add(gauge)) {
        metricRegistry.register(MetricRegistry.name(AsyncRequestHandler.class, pos + "-RequestsInFlight"), gauge);
      } else {
        logger.warn("Failed to register AsyncRequestHandler to the requests in flight tracker");
        metricAdditionError.inc();
      }
    }
  }

  /**
   * Tracks the state of the {@link RestRequestHandler}s provided as input and periodically reports how many of them are
   * alive and well.
   * @param requestHandlers the list of {@link RestRequestHandler}s whose state needs to be reported.
   */
  public void trackRequestHandlerHealth(final List<RestRequestHandler> requestHandlers) {
    requestHandlersAlive = new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        int count = 0;
        for (RestRequestHandler requestHandler : requestHandlers) {
          if (requestHandler.isRunning()) {
            count++;
          }
        }
        return count;
      }
    };
    metricRegistry
        .register(MetricRegistry.name(RequestHandlerController.class, "RequestHandlersAlive"), requestHandlersAlive);
  }
}
