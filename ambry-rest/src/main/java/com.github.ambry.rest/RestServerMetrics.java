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
public class RestServerMetrics {
  private final MetricRegistry metricRegistry;
  private final AtomicInteger asyncHandlerWorkerIndex = new AtomicInteger(0);

  // Rates
  // AsyncHandlerWorker
  public final Meter requestArrivalRate;
  public final Meter requestDequeuingRate;
  public final Meter requestQueuingRate;
  public final Meter responseArrivalRate;
  public final Meter responseCompletionRate;
  public final Meter responseQueuingRate;

  // Latencies
  // AsyncHandlerWorker
  public final Histogram requestPreProcessingTimeInMs;
  public final Histogram responsePreProcessingTimeInMs;

  // Errors
  // AsyncHandlerWorker
  public final Counter requestProcessingError;
  public final Counter requestQueueAddError;
  public final Counter resourceReleaseError;
  public final Counter responseAlreadyInFlightError;
  public final Counter responseCompleteTasksError;
  public final Counter responseProcessingError;
  public final Counter unexpectedError;
  public final Counter unknownRestMethodError;
  // AsyncRequestResponseHandler
  public final Counter requestResponseHandlerShutdownError;
  public final Counter requestResponseHandlerUnavailableError;
  // AsyncResponseInfo
  public final Counter trackingError;
  // RestServer
  public final Counter restServerInstantiationError;

  // Others
  // AsyncaHandlerWorker
  public final Counter responseExceptionCount;
  // AsyncRequestResponseHandler
  public final Histogram workerShutdownTimeInMs;
  public final Histogram workerStartTimeInMs;
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
  public RestServerMetrics(MetricRegistry metricRegistry) {
    this.metricRegistry = metricRegistry;

    // Rates
    // AsyncHandlerWorker
    requestArrivalRate = metricRegistry.meter(MetricRegistry.name(AsyncHandlerWorker.class, "RequestArrivalRate"));
    requestDequeuingRate = metricRegistry.meter(MetricRegistry.name(AsyncHandlerWorker.class, "RequestDequeuingRate"));
    requestQueuingRate = metricRegistry.meter(MetricRegistry.name(AsyncHandlerWorker.class, "RequestQueuingRate"));
    responseArrivalRate = metricRegistry.meter(MetricRegistry.name(AsyncHandlerWorker.class, "ResponseArrivalRate"));
    responseCompletionRate =
        metricRegistry.meter(MetricRegistry.name(AsyncHandlerWorker.class, "ResponseCompletionRate"));
    responseQueuingRate = metricRegistry.meter(MetricRegistry.name(AsyncHandlerWorker.class, "ResponseQueuingRate"));

    // Latencies
    // AsyncHandlerWorker
    requestPreProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AsyncHandlerWorker.class, "RequestPreProcessingTimeInMs"));
    responsePreProcessingTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AsyncHandlerWorker.class, "ResponsePreProcessingTimeInMs"));

    // Errors
    // AsyncHandlerWorker
    requestProcessingError =
        metricRegistry.counter(MetricRegistry.name(AsyncHandlerWorker.class, "RequestProcessingError"));
    requestQueueAddError =
        metricRegistry.counter(MetricRegistry.name(AsyncHandlerWorker.class, "RequestQueueAddError"));
    resourceReleaseError =
        metricRegistry.counter(MetricRegistry.name(AsyncHandlerWorker.class, "ResourceReleaseError"));
    responseAlreadyInFlightError =
        metricRegistry.counter(MetricRegistry.name(AsyncHandlerWorker.class, "ResponseAlreadyInFlightError"));
    responseCompleteTasksError =
        metricRegistry.counter(MetricRegistry.name(AsyncHandlerWorker.class, "ResponseCompleteTasksError"));
    responseProcessingError =
        metricRegistry.counter(MetricRegistry.name(AsyncHandlerWorker.class, "ResponseProcessingError"));
    unexpectedError = metricRegistry.counter(MetricRegistry.name(AsyncHandlerWorker.class, "UnexpectedError"));
    unknownRestMethodError =
        metricRegistry.counter(MetricRegistry.name(AsyncHandlerWorker.class, "UnknownRestMethodError"));
    // AsyncRequestResponseHandler
    requestResponseHandlerShutdownError =
        metricRegistry.counter(MetricRegistry.name(AsyncRequestResponseHandler.class, "ShutdownError"));
    requestResponseHandlerUnavailableError =
        metricRegistry.counter(MetricRegistry.name(AsyncRequestResponseHandler.class, "UnavailableError"));
    // AsyncResponseInfo
    trackingError = metricRegistry.counter(MetricRegistry.name(AsyncResponseInfo.class, "TrackingError"));
    // RestServer
    restServerInstantiationError = metricRegistry.counter(MetricRegistry.name(RestServer.class, "InstantiationError"));

    // Others
    // AsyncHandlerWorker
    responseExceptionCount =
        metricRegistry.counter(MetricRegistry.name(AsyncHandlerWorker.class, "ResponseExceptionCount"));
    // AsyncRequestResponseHandler
    workerShutdownTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AsyncRequestResponseHandler.class, "WorkerShutdownTimeInMs"));
    workerStartTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AsyncRequestResponseHandler.class, "WorkerStartTimeInMs"));
    requestResponseHandlerShutdownTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AsyncRequestResponseHandler.class, "ShutdownTimeInMs"));
    requestResponseHandlerStartTimeInMs =
        metricRegistry.histogram(MetricRegistry.name(AsyncRequestResponseHandler.class, "StartTimeInMs"));
    residualRequestQueueSize =
        metricRegistry.counter(MetricRegistry.name(AsyncHandlerWorker.class, "ResidualRequestQueueSize"));
    residualResponseSetSize =
        metricRegistry.counter(MetricRegistry.name(AsyncHandlerWorker.class, "ResidualResponseSetSize"));
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
  }

  /**
   * Registers the {@code asyncHandlerWorker} so that its metrics (request/response queue sizes) can be tracked.
   * @param asyncHandlerWorker the {@link AsyncHandlerWorker} whose metrics need to be tracked.
   */
  public void registerAsyncHandlerWorker(final AsyncHandlerWorker asyncHandlerWorker) {
    int pos = asyncHandlerWorkerIndex.getAndIncrement();
    Gauge<Integer> gauge = new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return asyncHandlerWorker.getRequestQueueSize();
      }
    };
    metricRegistry.register(MetricRegistry.name(AsyncHandlerWorker.class, pos + "-RequestQueueSize"), gauge);
    gauge = new Gauge<Integer>() {
      @Override
      public Integer getValue() {
        return asyncHandlerWorker.getResponseSetSize();
      }
    };
    metricRegistry.register(MetricRegistry.name(AsyncHandlerWorker.class, pos + "-ResponseSetSize"), gauge);
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
