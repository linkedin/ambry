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
 * {@link RestServer} and Rest infrastructure ({@link RequestResponseHandlerController}, {@link AsyncRequestResponseHandler})
 * specific metrics tracking.
 * <p/>
 * Exports metrics that are triggered by Rest infrastructure to the provided {@link MetricRegistry}.
 */
public class RestServerMetrics {
  private final MetricRegistry metricRegistry;
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final Object asyncRequestResponseHandlerRegister = new Object();

  // Gauges
  // AsyncRequestResponseHandler
  private final List<Gauge<Integer>> asyncRequestQueueOccupancyGauges;
  private final List<Gauge<Integer>> asyncResponseListOccupancyGauges;
  private Gauge<Integer> requestResponseHandlersAlive;

  // Rates
  // AsyncRequestResponseHandler
  public final Meter asyncRequestHandlerQueueingRate;
  public final Meter asyncRequestHandlerRequestArrivalRate;
  // AsyncHandlerWorker
  public final Meter dequeuedRequestHandlerDequeuingRate;
  public final Meter dequeuedRequestHandlerRequestCompletionRate;

  // Latencies

  // Errors
  private final Counter metricAdditionError;
  // RequestResponseHandlerController
  public final Counter requestHandlerControllerInstantiationError;
  public final Counter requestHandlerSelectionError;
  // AsyncRequestResponseHandler
  public final Counter asyncRequestHandlerQueueAddError;
  public final Counter asyncRequestHandlerRestRequestInfoNullError;
  public final Counter asyncRequestHandlerRestResponseChannelNullError;
  public final Counter asyncRequestHandlerRestRequestNullError;
  public final Counter asyncRequestHandlerUnavailableError;
  public final Counter asyncRequestHandlerRequestAlreadyInFlightError;
  public final Counter asyncRequestHandlerShutdownError;
  public final Counter asyncRequestHandlerResidualRequestQueueSize;
  // AsyncHandlerWorker
  public final Counter dequeuedRequestHandlerUnknownRestMethodError;
  public final Counter dequeuedRequestHandlerRequestHandlingError;
  public final Counter dequeuedRequestHandlerQueuePollInterruptedError;
  public final Counter dequeuedRequestHandlerUnexpectedExceptionError;
  public final Counter dequeuedRequestHandlerHandlingCompleteTasksError;
  // RestServer
  public final Counter restServerInstantiationError;

  // Others
  // AsyncRequestResponseHandler
  public final Counter asyncRequestHandlerForcedShutdown;
  public final Histogram asyncRequestHandlerQueueTimeInMs;
  public final Histogram asyncRequestHandlerShutdownTimeInMs;
  // RestServer
  public final Histogram restServerStartTimeInMs;
  public final Histogram restServerShutdownTimeInMs;

  /**
   * Creates an instance of RestServerMetrics using the given {@code metricRegistry}.
   * @param metricRegistry the {@link MetricRegistry} to use for the metrics.
   */
  public RestServerMetrics(MetricRegistry metricRegistry) {
    this.metricRegistry = metricRegistry;
    asyncRequestQueueOccupancyGauges = new ArrayList<Gauge<Integer>>();
    asyncResponseListOccupancyGauges = new ArrayList<Gauge<Integer>>();

    asyncRequestHandlerQueueingRate =
        metricRegistry.meter(MetricRegistry.name(AsyncRequestResponseHandler.class, "QueueingRate"));
    asyncRequestHandlerRequestArrivalRate =
        metricRegistry.meter(MetricRegistry.name(AsyncRequestResponseHandler.class, "RequestArrivalRate"));
    dequeuedRequestHandlerDequeuingRate =
        metricRegistry.meter(MetricRegistry.name(AsyncHandlerWorker.class, "DequeuingRate"));
    dequeuedRequestHandlerRequestCompletionRate =
        metricRegistry.meter(MetricRegistry.name(AsyncHandlerWorker.class, "RequestCompletionRate"));

    metricAdditionError = metricRegistry.counter(MetricRegistry.name(getClass(), "MetricAdditionError"));
    requestHandlerControllerInstantiationError =
        metricRegistry.counter(MetricRegistry.name(RequestResponseHandlerController.class, "InstantiationError"));
    requestHandlerSelectionError = metricRegistry
        .counter(MetricRegistry.name(RequestResponseHandlerController.class, "RequestHandlerSelectionError"));
    asyncRequestHandlerQueueAddError =
        metricRegistry.counter(MetricRegistry.name(AsyncRequestResponseHandler.class, "QueueAddError"));
    asyncRequestHandlerRestRequestInfoNullError =
        metricRegistry.counter(MetricRegistry.name(AsyncRequestResponseHandler.class, "RestRequestInfoNullError"));
    asyncRequestHandlerRestResponseChannelNullError =
        metricRegistry.counter(MetricRegistry.name(AsyncRequestResponseHandler.class, "ResponseChannelNullError"));
    asyncRequestHandlerRestRequestNullError =
        metricRegistry.counter(MetricRegistry.name(AsyncRequestResponseHandler.class, "RestRequestNullError"));
    asyncRequestHandlerUnavailableError =
        metricRegistry.counter(MetricRegistry.name(AsyncRequestResponseHandler.class, "UnavailableError"));
    asyncRequestHandlerRequestAlreadyInFlightError =
        metricRegistry.counter(MetricRegistry.name(AsyncRequestResponseHandler.class, "RequestAlreadyInFlightError"));
    asyncRequestHandlerShutdownError =
        metricRegistry.counter(MetricRegistry.name(AsyncRequestResponseHandler.class, "ShutdownError"));
    asyncRequestHandlerResidualRequestQueueSize =
        metricRegistry.counter(MetricRegistry.name(AsyncRequestResponseHandler.class, "ResidualQueueSize"));
    dequeuedRequestHandlerUnknownRestMethodError =
        metricRegistry.counter(MetricRegistry.name(AsyncHandlerWorker.class, "UnknownRestMethodError"));
    dequeuedRequestHandlerRequestHandlingError =
        metricRegistry.counter(MetricRegistry.name(AsyncHandlerWorker.class, "RestRequestInfoHandlingError"));
    dequeuedRequestHandlerQueuePollInterruptedError =
        metricRegistry.counter(MetricRegistry.name(AsyncHandlerWorker.class, "QueueTakeInterruptedError"));
    dequeuedRequestHandlerUnexpectedExceptionError =
        metricRegistry.counter(MetricRegistry.name(AsyncHandlerWorker.class, "UnexpectedExceptionError"));
    dequeuedRequestHandlerHandlingCompleteTasksError =
        metricRegistry.counter(MetricRegistry.name(AsyncHandlerWorker.class, "OnHandlingCompleteTasksError"));
    restServerInstantiationError = metricRegistry.counter(MetricRegistry.name(RestServer.class, "InstantiationError"));

    asyncRequestHandlerForcedShutdown =
        metricRegistry.counter(MetricRegistry.name(AsyncRequestResponseHandler.class, "ForcedShutdown"));
    asyncRequestHandlerQueueTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AsyncRequestResponseHandler.class, "QueueTimeInMs"));
    asyncRequestHandlerShutdownTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AsyncRequestResponseHandler.class, "ShutdownTimeInMs"));
    restServerStartTimeInMs = metricRegistry.histogram(MetricRegistry.name(RestServer.class, "StartTimeInMs"));
    restServerShutdownTimeInMs = metricRegistry.histogram(MetricRegistry.name(RestServer.class, "ShutdownTimeInMs"));
  }

  /**
   * Registers an {@link AsyncRequestResponseHandler} so that its metrics (queue occupancy, requests in flight) can be tracked.
   * @param requestResponseHandler the {@link AsyncRequestResponseHandler} whose metrics need to be tracked.
   */
  public void registerAsyncRequestResponseHandler(final AsyncRequestResponseHandler requestResponseHandler) {
    synchronized (asyncRequestResponseHandlerRegister) {
      assert asyncRequestQueueOccupancyGauges.size() == asyncResponseListOccupancyGauges.size();
      int pos = asyncRequestQueueOccupancyGauges.size();
      Gauge<Integer> gauge = new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          return requestResponseHandler.getRequestQueueSize();
        }
      };
      if (asyncRequestQueueOccupancyGauges.add(gauge)) {
        metricRegistry.register(MetricRegistry.name(AsyncRequestResponseHandler.class, pos + "-QueueOccupancy"), gauge);
      } else {
        logger.warn("Failed to register AsyncRequestResponseHandler to the queue occupancy tracker");
        metricAdditionError.inc();
      }

      gauge = new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          return requestResponseHandler.getResponseSetSize();
        }
      };
      if (asyncResponseListOccupancyGauges.add(gauge)) {
        metricRegistry
            .register(MetricRegistry.name(AsyncRequestResponseHandler.class, pos + "-RequestsInFlight"), gauge);
      } else {
        logger.warn("Failed to register AsyncRequestResponseHandler to the requests in flight tracker");
        metricAdditionError.inc();
      }
    }
  }

  /**
   * Tracks the state of the {@link AsyncRequestResponseHandler}s provided as input and periodically reports how many of
   * them are alive and well.
   * @param requestResponseHandlers the list of {@link AsyncRequestResponseHandler}s whose state needs to be reported.
   */
  public void trackRequestHandlerHealth(final List<AsyncRequestResponseHandler> requestResponseHandlers) {
    requestResponseHandlersAlive = new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        int count = 0;
        for (AsyncRequestResponseHandler requestResponseHandler : requestResponseHandlers) {
          if (requestResponseHandler.isRunning()) {
            count++;
          }
        }
        return count;
      }
    };
    metricRegistry.register(MetricRegistry.name(RequestResponseHandlerController.class, "RequestResponseHandlersAlive"),
        requestResponseHandlersAlive);
  }
}
