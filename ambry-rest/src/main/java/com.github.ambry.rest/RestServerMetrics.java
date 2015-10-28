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
  private final Object requestResponseHandlerRegister = new Object();

  // Gauges
  // AsyncRequestResponseHandler
  private final List<Gauge<Integer>> requestQueueSizeGauges;
  private final List<Gauge<Integer>> responseSetSizeGauges;

  // Rates
  // AsyncHandlerWorker
  public final Meter requestArrivalRate;
  public final Meter requestDequeueingRate;
  public final Meter requestQueueingRate;
  public final Meter responseArrivalRate;
  public final Meter responseCompletionRate;
  public final Meter responseQueueingRate;

  // Latencies

  // Errors
  // AsyncHandlerWorker
  public final Counter requestProcessingError;
  public final Counter requestQueueOfferError;
  public final Counter residualRequestQueueSize;
  public final Counter residualResponseSetSize;
  public final Counter resourceReleaseError;
  public final Counter responseAlreadyInFlight;
  public final Counter responseCompleteTasksError;
  public final Counter responseProcessingError;
  public final Counter unexpectedException;
  public final Counter unknownRestMethod;
  // AsyncRequestResponseHandler
  public final Counter requestResponseHandlerShutdownError;
  public final Counter requestResponseHandlerUnavailable;
  // RestServer
  public final Counter restServerInstantiationError;
  // RestServerMetrics
  private final Counter metricAdditionError;

  // Others
  // AsyncRequestResponseHandler
  public final Histogram requestResponseHandlerShutdownTimeInMs;
  // RestServer
  public final Histogram restServerShutdownTimeInMs;
  public final Histogram restServerStartTimeInMs;

  public RestServerMetrics(MetricRegistry metricRegistry) {
    this.metricRegistry = metricRegistry;

    // Gauges
    requestQueueSizeGauges = new ArrayList<Gauge<Integer>>();
    responseSetSizeGauges = new ArrayList<Gauge<Integer>>();

    // Rates
    // AsyncHandlerWorker
    requestArrivalRate =
        metricRegistry.meter(MetricRegistry.name(AsyncHandlerWorker.class, "RequestArrivalRate"));
    requestDequeueingRate = metricRegistry.meter(MetricRegistry.name(AsyncHandlerWorker.class, "RequestDequeuingRate"));
    requestQueueingRate =
        metricRegistry.meter(MetricRegistry.name(AsyncHandlerWorker.class, "RequestQueueingRate"));
    responseArrivalRate =
        metricRegistry.meter(MetricRegistry.name(AsyncHandlerWorker.class, "ResponseArrivalRate"));
    responseCompletionRate =
        metricRegistry.meter(MetricRegistry.name(AsyncHandlerWorker.class, "ResponseCompletionRate"));
    responseQueueingRate =
        metricRegistry.meter(MetricRegistry.name(AsyncHandlerWorker.class, "ResponseQueueingRate"));

    // Latencies

    // Errors
    // AsyncHandlerWorker
    requestProcessingError =
        metricRegistry.counter(MetricRegistry.name(AsyncHandlerWorker.class, "RequestProcessingError"));
    requestQueueOfferError =
        metricRegistry.counter(MetricRegistry.name(AsyncHandlerWorker.class, "RequestQueueOfferError"));
    residualRequestQueueSize =
        metricRegistry.counter(MetricRegistry.name(AsyncHandlerWorker.class, "ResidualRequestQueueSize"));
    residualResponseSetSize =
        metricRegistry.counter(MetricRegistry.name(AsyncHandlerWorker.class, "ResidualResponseSetSize"));
    resourceReleaseError = metricRegistry.counter(MetricRegistry.name(AsyncHandlerWorker.class, "ResourceReleaseError"));
    responseAlreadyInFlight =
        metricRegistry.counter(MetricRegistry.name(AsyncHandlerWorker.class, "ResponseAlreadyInFlightError"));
    responseCompleteTasksError =
        metricRegistry.counter(MetricRegistry.name(AsyncHandlerWorker.class, "ResponseCompleteTasksError"));
    responseProcessingError =
        metricRegistry.counter(MetricRegistry.name(AsyncHandlerWorker.class, "ResponseProcessingError"));
    unexpectedException =
        metricRegistry.counter(MetricRegistry.name(AsyncHandlerWorker.class, "UnexpectedExceptionError"));
    unknownRestMethod = metricRegistry.counter(MetricRegistry.name(AsyncHandlerWorker.class, "UnknownRestMethodError"));
    // AsyncRequestResponseHandler
    requestResponseHandlerShutdownError =
        metricRegistry.counter(MetricRegistry.name(AsyncRequestResponseHandler.class, "ShutdownError"));
    requestResponseHandlerUnavailable =
        metricRegistry.counter(MetricRegistry.name(AsyncRequestResponseHandler.class, "UnavailableError"));
    // RestServer
    restServerInstantiationError = metricRegistry.counter(MetricRegistry.name(RestServer.class, "InstantiationError"));
    // RestServerMetrics
    metricAdditionError = metricRegistry.counter(MetricRegistry.name(getClass(), "MetricAdditionError"));

    // Others
    // AsyncRequestResponseHandler
    requestResponseHandlerShutdownTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AsyncRequestResponseHandler.class, "ShutdownTimeInMs"));
    // RestServer
    restServerShutdownTimeInMs = metricRegistry.histogram(MetricRegistry.name(RestServer.class, "ShutdownTimeInMs"));
    restServerStartTimeInMs = metricRegistry.histogram(MetricRegistry.name(RestServer.class, "StartTimeInMs"));
  }

  /**
   * Registers an {@link AsyncRequestResponseHandler} so that its metrics (request/response queue sizes) can be tracked.
   * @param requestResponseHandler the {@link AsyncRequestResponseHandler} whose metrics need to be tracked.
   */
  public void registerAsyncRequestResponseHandler(final AsyncRequestResponseHandler requestResponseHandler) {
    synchronized (requestResponseHandlerRegister) {
      assert requestQueueSizeGauges.size() == responseSetSizeGauges.size();
      int pos = requestQueueSizeGauges.size();
      Gauge<Integer> gauge = new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          return requestResponseHandler.getRequestQueueSize();
        }
      };
      if (requestQueueSizeGauges.add(gauge)) {
        metricRegistry
            .register(MetricRegistry.name(AsyncHandlerWorker.class, pos + "-RequestQueueSize"), gauge);
      } else {
        logger.warn("Failed to register AsyncRequestResponseHandler to the request queue size tracker");
        metricAdditionError.inc();
      }

      gauge = new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          return requestResponseHandler.getResponseSetSize();
        }
      };
      if (responseSetSizeGauges.add(gauge)) {
        metricRegistry
            .register(MetricRegistry.name(AsyncHandlerWorker.class, pos + "-ResponseSetSize"), gauge);
      } else {
        logger.warn("Failed to register AsyncRequestResponseHandler to the response set size tracker");
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
    Gauge<Integer> requestResponseHandlersAlive = new Gauge<Integer>() {
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
