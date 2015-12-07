package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Construct to support end-to-end metrics tracking based on request type. Usually accompanies a single
 * {@link RestRequest} i.e. there is a one-to-one mapping b/w a {@link RestRequest} and a RestRequestMetricsTracker
 * instance.
 */
public class RestRequestMetricsTracker {
  protected static final String DEFAULT_REQUEST_TYPE = "Unknown";

  private static RestRequestMetrics defaultMetrics;

  /**
   * Metrics that should be updated in the NIO layer.
   */
  public final NioLayerMetrics nioLayerMetrics = new NioLayerMetrics();
  /**
   * Metrics that should be updated in the scaling layer.
   */
  public final ScalingLayerMetrics scalingLayerMetrics = new ScalingLayerMetrics();

  private final AtomicLong totalCpuTimeInMs = new AtomicLong(0);
  private final AtomicBoolean metricsRecorded = new AtomicBoolean(false);
  private RestRequestMetrics metrics = defaultMetrics;

  /**
   * Metrics that are updated at the NIO layer.
   */
  public class NioLayerMetrics {
    private final AtomicLong requestProcessingTimeInMs = new AtomicLong(0);
    private final AtomicLong responseProcessingTimeInMs = new AtomicLong(0);

    private long requestReceivedTime = 0;
    private long roundTripTimeInMs = 0;

    /**
     * Adds to the time taken to process the request at the NIO layer. Also adds to the total time taken to service the
     * request.
     * @param delta the time taken in ms to do the current piece of processing at the NIO layer for the request.
     * @return the total time taken in ms to process the request at the NIO layer, including the current piece, at this
     *          moment.
     */
    public long addToRequestProcessingTime(long delta) {
      addToTotalCpuTime(delta);
      return requestProcessingTimeInMs.addAndGet(delta);
    }

    /**
     * Adds to the time taken to process the response at the NIO layer. Also adds to the total time taken to service the
     * request.
     * @param delta the time taken in ms to do the current piece of processing at the NIO layer for the response.
     * @return the total time taken in ms to process the response at the NIO layer, including the current piece, at this
     *          moment.
     */
    public long addToResponseProcessingTime(long delta) {
      addToTotalCpuTime(delta);
      return responseProcessingTimeInMs.addAndGet(delta);
    }

    /**
     * Marks the time at which the request was received.
     */
    public void markRequestReceived() {
      requestReceivedTime = System.currentTimeMillis();
    }

    /**
     * Marks the time at which request was completed so that request RTT can be calculated.
     */
    public void markRequestCompleted() {
      if (requestReceivedTime == 0) {
        throw new IllegalStateException("Request was marked completed without being marked received");
      }
      roundTripTimeInMs = System.currentTimeMillis() - requestReceivedTime;
    }
  }

  /**
   * Metrics that are updated at the scaling layer.
   */
  public class ScalingLayerMetrics {
    private final AtomicLong requestProcessingTimeInMs = new AtomicLong(0);
    private final AtomicLong requestQueuingTimeInMs = new AtomicLong(0);
    private final AtomicLong responseProcessingTimeInMs = new AtomicLong(0);
    private final AtomicLong responseQueuingTimeInMs = new AtomicLong(0);

    private long requestReceivedTime = 0;
    private long roundTripTimeInMs = 0;

    /**
     * Adds to the time taken to process a request at the scaling layer. Also adds to the total time taken to service
     * the request.
     * @param delta the time taken in ms to do the current piece of processing at the scaling layer for the request.
     * @return the total time taken in ms to process this request at the scaling layer, including the current piece, at
     *          this moment.
     */
    public long addToRequestProcessingTime(long delta) {
      addToTotalCpuTime(delta);
      return requestProcessingTimeInMs.addAndGet(delta);
    }

    /**
     * Adds to the scaling layer queuing time of a request. Also adds to the total time taken to service the request.
     * @param delta the time in ms a request has spent being queued at the scaling layer.
     * @return the total time in ms this request has spent being queued at the scaling layer at this moment.
     */
    public long addToRequestQueuingTime(long delta) {
      addToTotalCpuTime(delta);
      return requestQueuingTimeInMs.addAndGet(delta);
    }

    /**
     * Adds to the time taken to process a response at the scaling layer. Also adds to the total time taken to service
     * the request.
     * @param delta the time taken in ms to do the current piece of processing at the scaling layer for the response.
     * @return the total time taken in ms to process the response at the scaling layer, including the current piece, at
     *          this moment.
     */
    public long addToResponseProcessingTime(long delta) {
      addToTotalCpuTime(delta);
      return responseProcessingTimeInMs.addAndGet(delta);
    }

    /**
     * Adds to the scaling layer queuing time of a response. Also adds to the total time taken to service the request.
     * @param delta the time in ms a response has spent being queued at the scaling layer.
     * @return the total time in ms this response has spent being queued at the scaling layer at this moment.
     */
    public long addToResponseQueuingTime(long delta) {
      addToTotalCpuTime(delta);
      return responseQueuingTimeInMs.addAndGet(delta);
    }

    /**
     * Marks the time at which the request was received.
     */
    public void markRequestReceived() {
      requestReceivedTime = System.currentTimeMillis();
    }

    /**
     * Marks the time at which request was completed so that request RTT can be calculated.
     */
    public void markRequestCompleted() {
      if (requestReceivedTime == 0) {
        throw new IllegalStateException("Request was marked completed without being marked received");
      }
      roundTripTimeInMs = System.currentTimeMillis() - requestReceivedTime;
    }
  }

  /**
   * Adds to the total cpu time taken in ms to service the request.
   * @param delta the time taken in ms to do the current piece of processing for the request.
   * @return the total cpu time taken in ms to service the request across all layers, including the current piece,
   *          at this moment.
   */
  public long addToTotalCpuTime(long delta) {
    return totalCpuTimeInMs.addAndGet(delta);
  }

  /**
   * Injects a {@link RestRequestMetrics} that can be used to track the metrics of the {@link RestRequest} that this
   * instance of RestRequestMetricsTracker is attached to.
   * @param restRequestMetrics the {@link RestRequestMetrics} instance to use to track the metrics of the
   *                                {@link RestRequest} that this instance of RestRequestMetricsTracker is attached to.
   */
  public void injectMetrics(RestRequestMetrics restRequestMetrics) {
    if (restRequestMetrics != null) {
      metrics = restRequestMetrics;
    } else {
      throw new IllegalArgumentException("RestRequestMetrics provided cannot be null");
    }
  }

  /**
   * Records the metrics.
   * </p>
   * This method is expected to called when the {@link RestRequest}, that this instance of {@link RestRequestMetricsTracker} is
   * attached to, finishes.
   */
  public void recordMetrics() {
    if (metrics != null) {
      if (metricsRecorded.compareAndSet(false, true)) {
        metrics.nioRequestProcessingTimeInMs.update(nioLayerMetrics.requestProcessingTimeInMs.get());
        metrics.nioResponseProcessingTimeInMs.update(nioLayerMetrics.responseProcessingTimeInMs.get());
        metrics.nioRoundTripTimeInMs.update(nioLayerMetrics.roundTripTimeInMs);

        metrics.scRequestProcessingTimeInMs.update(scalingLayerMetrics.requestProcessingTimeInMs.get());
        metrics.scRequestQueuingTimeInMs.update(scalingLayerMetrics.requestQueuingTimeInMs.get());
        metrics.scResponseProcessingTimeInMs.update(scalingLayerMetrics.responseProcessingTimeInMs.get());
        metrics.scResponseQueuingTimeInMs.update(scalingLayerMetrics.responseQueuingTimeInMs.get());
        metrics.scRoundTripTimeInMs.update(scalingLayerMetrics.roundTripTimeInMs);

        metrics.totalCpuTimeInMs.update(totalCpuTimeInMs.get());
      }
    } else {
      throw new IllegalStateException("Could not record metrics because there is no metrics tracker");
    }
  }

  /**
   * Creates a default {@link RestRequestMetrics} in case {@link #injectMetrics(RestRequestMetrics)} is never
   * called on an instance of {@link RestRequestMetricsTracker}.
   * @param metricRegistry the {@link MetricRegistry} to use to register the created metrics.
   */
  public static void setDefaults(MetricRegistry metricRegistry) {
    defaultMetrics = new RestRequestMetrics(RestRequestMetricsTracker.class, DEFAULT_REQUEST_TYPE, metricRegistry);
  }
}
