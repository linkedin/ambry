package com.github.ambry.rest;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Construct to support end-to-end metrics tracking based on request type. Usually accompanies a single
 * {@link RestRequest} i.e. there is a one-to-one mapping b/w a {@link RestRequest} and a RestRequestMetrics instance.
 */
public class RestRequestMetrics {
  protected static final String DEFAULT_REQUEST_TYPE = "Unknown";

  protected static final String NIO_REQUEST_PROCESSING_TIME_SUFFIX = "NioRequestProcessingTime";
  protected static final String NIO_RESPONSE_PROCESSING_TIME_SUFFIX = "NioResponseProcessingTime";

  protected static final String SC_REQUEST_PROCESSING_TIME_SUFFIX = "ScRequestProcessingTime";
  protected static final String SC_REQUEST_QUEUING_TIME_SUFFIX = "ScRequestQueuingTime";
  protected static final String SC_RESPONSE_PROCESSING_TIME_SUFFIX = "ScResponseProcessingTime";
  protected static final String SC_RESPONSE_QUEUING_TIME_SUFFIX = "ScResponseQueuingTime";

  protected static final String TOTAL_TIME_SUFFIX = "TotalTime";

  private static RequestMetricsTracker defaultMetricsTracker;

  /**
   * Metrics that should be updated in the NIO layer.
   */
  public final NioLayerMetrics nioLayerMetrics = new NioLayerMetrics();
  /**
   * Metrics that should be updated in the scaling layer.
   */
  public final ScalingLayerMetrics scalingLayerMetrics = new ScalingLayerMetrics();

  private final AtomicLong totalTime = new AtomicLong(0);
  private final AtomicBoolean metricsRecorded = new AtomicBoolean(false);
  private RequestMetricsTracker metricsTracker = defaultMetricsTracker;

  /**
   * A metrics tracker that is provided as input to {@link #injectTracker(RequestMetricsTracker)}.
   * </p>
   * It is expected that each request type will have it's own instance of RequestMetricsTracker and the same instance is
   * used to track all requests of that type.
   */
  public static class RequestMetricsTracker {
    private final Histogram nioRequestProcessingTime;
    private final Histogram nioResponseProcessingTime;

    private final Histogram scRequestProcessingTime;
    private final Histogram scRequestQueuingTime;
    private final Histogram scResponseProcessingTime;
    private final Histogram scResponseQueuingTime;

    private final Histogram totalTime;

    /**
     * Creates an instance of RequestMetricsTracker for {@code requestType} and attaches all the metrics related to the
     * request to the given {@code ownerClass}. The metrics are also registered in the provided {@code metricRegistry}.
     * @param ownerClass the {@link Class} that is supposed to own the metrics created by this tracker.
     * @param requestType the type of request for which a tracker is being created.
     * @param metricRegistry the {@link MetricRegistry} to use to register the created metrics.
     */
    public RequestMetricsTracker(Class ownerClass, String requestType, MetricRegistry metricRegistry) {
      if (ownerClass == null || requestType == null || metricRegistry == null) {
        throw new IllegalArgumentException(
            "Null arg(s) during instantiation. Owner class - [" + ownerClass + "]. Request type - [" + requestType
                + "]. Metric registry - [" + metricRegistry + "]");
      }

      nioRequestProcessingTime =
          metricRegistry.histogram(MetricRegistry.name(ownerClass, requestType + NIO_REQUEST_PROCESSING_TIME_SUFFIX));
      nioResponseProcessingTime =
          metricRegistry.histogram(MetricRegistry.name(ownerClass, requestType + NIO_RESPONSE_PROCESSING_TIME_SUFFIX));

      scRequestProcessingTime =
          metricRegistry.histogram(MetricRegistry.name(ownerClass, requestType + SC_REQUEST_PROCESSING_TIME_SUFFIX));
      scRequestQueuingTime =
          metricRegistry.histogram(MetricRegistry.name(ownerClass, requestType + SC_REQUEST_QUEUING_TIME_SUFFIX));
      scResponseProcessingTime =
          metricRegistry.histogram(MetricRegistry.name(ownerClass, requestType + SC_RESPONSE_PROCESSING_TIME_SUFFIX));
      scResponseQueuingTime =
          metricRegistry.histogram(MetricRegistry.name(ownerClass, requestType + SC_RESPONSE_QUEUING_TIME_SUFFIX));

      totalTime = metricRegistry.histogram(MetricRegistry.name(ownerClass, requestType + TOTAL_TIME_SUFFIX));
    }
  }

  /**
   * Metrics that are updated at the NIO layer.
   */
  public class NioLayerMetrics {
    private final AtomicLong requestProcessingTime = new AtomicLong(0);
    private final AtomicLong responseProcessingTime = new AtomicLong(0);

    /**
     * Adds to the time taken to process the request at the NIO layer. Also adds to the total time taken to service the
     * request.
     * @param delta the time taken in ms to do the current piece of processing at the NIO layer for the request.
     * @return the total time taken in ms to process the request at the NIO layer, including the current piece, at this
     *          moment.
     */
    public long addToRequestProcessingTime(long delta) {
      addToTotalTime(delta);
      return requestProcessingTime.addAndGet(delta);
    }

    /**
     * Adds to the time taken to process the response at the NIO layer. Also adds to the total time taken to service the
     * request.
     * @param delta the time taken in ms to do the current piece of processing at the NIO layer for the response.
     * @return the total time taken in ms to process the response at the NIO layer, including the current piece, at this
     *          moment.
     */
    public long addToResponseProcessingTime(long delta) {
      addToTotalTime(delta);
      return responseProcessingTime.addAndGet(delta);
    }
  }

  /**
   * Metrics that are updated at the scaling layer.
   */
  public class ScalingLayerMetrics {
    private final AtomicLong requestProcessingTime = new AtomicLong(0);
    private final AtomicLong requestQueuingTime = new AtomicLong(0);
    private final AtomicLong responseProcessingTime = new AtomicLong(0);
    private final AtomicLong responseQueuingTime = new AtomicLong(0);

    /**
     * Adds to the time taken to process a request at the scaling layer. Also adds to the total time taken to service
     * the request.
     * @param delta the time taken in ms to do the current piece of processing at the scaling layer for the request.
     * @return the total time taken in ms to process this request at the scaling layer, including the current piece, at
     *          this moment.
     */
    public long addToRequestProcessingTime(long delta) {
      addToTotalTime(delta);
      return requestProcessingTime.addAndGet(delta);
    }

    /**
     * Adds to the scaling layer queuing time of a request. Also adds to the total time taken to service the request.
     * @param delta the time in ms a request has spent being queued at the scaling layer.
     * @return the total time in ms this request has spent being queued at the scaling layer at this moment.
     */
    public long addToRequestQueuingTime(long delta) {
      addToTotalTime(delta);
      return requestQueuingTime.addAndGet(delta);
    }

    /**
     * Adds to the time taken to process a response at the scaling layer. Also adds to the total time taken to service
     * the request.
     * @param delta the time taken in ms to do the current piece of processing at the scaling layer for the response.
     * @return the total time taken in ms to process the response at the scaling layer, including the current piece, at
     *          this moment.
     */
    public long addToResponseProcessingTime(long delta) {
      addToTotalTime(delta);
      return responseProcessingTime.addAndGet(delta);
    }

    /**
     * Adds to the scaling layer queuing time of a response. Also adds to the total time taken to service the request.
     * @param delta the time in ms a response has spent being queued at the scaling layer.
     * @return the total time in ms this response has spent being queued at the scaling layer at this moment.
     */
    public long addToResponseQueuingTime(long delta) {
      addToTotalTime(delta);
      return responseQueuingTime.addAndGet(delta);
    }
  }

  /**
   * Adds to the total time taken in ms to service the request.
   * @param delta the time taken in ms to do the current piece of processing for the request.
   * @return the total time taken in ms to service the request across all layers, including the current piece,
   *          at this moment.
   */
  public long addToTotalTime(long delta) {
    return totalTime.addAndGet(delta);
  }

  /**
   * Injects a {@link RequestMetricsTracker} that can be used to track the metrics of the {@link RestRequest} that this
   * instance of RestRequestMetrics is attached to.
   * @param requestMetricsTracker the {@link RequestMetricsTracker} instance to use to track the metrics of the
   *                                {@link RestRequest} that this instance of RestRequestMetrics is attached to.
   */
  public void injectTracker(RequestMetricsTracker requestMetricsTracker) {
    if (requestMetricsTracker != null) {
      metricsTracker = requestMetricsTracker;
    } else {
      throw new IllegalArgumentException("RequestMetricsTracker provided cannot be null");
    }
  }

  /**
   * Records the metrics.
   * </p>
   * This method is expected to called when the {@link RestRequest}, that this instance of {@link RestRequestMetrics} is
   * attached to, finishes.
   */
  public void recordMetrics() {
    if (metricsTracker != null) {
      if (metricsRecorded.compareAndSet(false, true)) {
        metricsTracker.nioRequestProcessingTime.update(nioLayerMetrics.requestProcessingTime.get());
        metricsTracker.nioResponseProcessingTime.update(nioLayerMetrics.responseProcessingTime.get());

        metricsTracker.scRequestProcessingTime.update(scalingLayerMetrics.requestProcessingTime.get());
        metricsTracker.scRequestQueuingTime.update(scalingLayerMetrics.requestQueuingTime.get());
        metricsTracker.scResponseProcessingTime.update(scalingLayerMetrics.responseProcessingTime.get());
        metricsTracker.scResponseQueuingTime.update(scalingLayerMetrics.responseQueuingTime.get());

        metricsTracker.totalTime.update(totalTime.get());
      }
    } else {
      throw new IllegalStateException("Could not record metrics because there is no metrics tracker");
    }
  }

  /**
   * Creates a default {@link RequestMetricsTracker} in case {@link #injectTracker(RequestMetricsTracker)} is never
   * called on an instance of {@link RestRequestMetrics}.
   * @param metricRegistry the {@link MetricRegistry} to use to register the created metrics.
   */
  public static void setDefaults(MetricRegistry metricRegistry) {
    defaultMetricsTracker = new RequestMetricsTracker(RestRequestMetrics.class, DEFAULT_REQUEST_TYPE, metricRegistry);
  }
}


