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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.frontend.ContainerMetrics;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Construct to support end-to-end metrics tracking based on request type. Usually accompanies a single
 * {@link RestRequest} i.e. there is a one-to-one mapping b/w a {@link RestRequest} and a RestRequestMetricsTracker
 * instance.
 * <p/>
 * A brief description of how the tracker works :-
 * 1. When an object of type {@link RestRequest} (request) is instantiated, it is also expected to be associated with a
 *    unique instance of RestRequestMetricsTracker (tracker). This unique instance is returned on every call to
 *    {@link RestRequest#getMetricsTracker()}. Therefore there is one-to-one mapping between an instance of
 *    {@link RestRequest} and an instance of RestRequestMetricsTracker.
 * 2. When the tracker is instantiated, the type of the request is usually not available. Therefore, the
 *    {@link RestRequestMetrics} (metrics) object associated with the tracker tracks "unknown" requests.
 * 3. As the request passes through the NIO framework and the scaling framework, metrics associated with these layers
 *    are tracked and stored but not updated in the metrics object.
 * 4. When the request reaches the {@link RestRequestService}, it is usually identified as a specific type and a custom
 *    metrics object that tracks that specific type of request is "injected" into the tracker associated with the
 *    request.
 * 5. When the response for the request is complete and the request is "closed", the metrics that are stored are
 *    updated in the metrics object (injected or default).
 */
public class RestRequestMetricsTracker {
  protected static final String DEFAULT_REQUEST_TYPE = "Unknown";

  private static RestRequestMetrics defaultMetrics;

  /**
   * NIO related metrics tracker instance.
   */
  public final NioMetricsTracker nioMetricsTracker = new NioMetricsTracker();
  /**
   * Scaling related metrics tracker instance.
   */
  public final ScalingMetricsTracker scalingMetricsTracker = new ScalingMetricsTracker();

  private final AtomicBoolean metricsRecorded = new AtomicBoolean(false);
  private RestRequestMetrics metrics = defaultMetrics;
  private ContainerMetrics containerMetrics;
  private boolean failed = false;
  private boolean satisfied = true;
  private ResponseStatus responseStatus = ResponseStatus.Ok;

  /**
   * Tracker for updating NIO related metrics.
   * </p>
   * These are usually updated in classes implemented by the {@link NioServer} framework (e.g. Implementations of
   * {@link RestRequest}, {@link RestResponseChannel} or any other classes that form a part of the framework).
   */
  public static class NioMetricsTracker {
    private final AtomicLong requestProcessingTimeInMs = new AtomicLong(0);
    private final AtomicLong responseProcessingTimeInMs = new AtomicLong(0);

    private long requestReceivedTime = 0;
    private long timeToFirstByteInMs = 0;
    private long roundTripTimeInMs = 0;

    /**
     * Adds to the time taken to process the request at the NIO layer.
     * @param delta the time taken in ms to do the current piece of processing at the NIO layer for the request.
     * @return the total time taken in ms to process the request at the NIO layer, including the current piece, at this
     *          moment.
     */
    public long addToRequestProcessingTime(long delta) {
      return requestProcessingTimeInMs.addAndGet(delta);
    }

    /**
     * Adds to the time taken to process the response at the NIO layer.
     * @param delta the time taken in ms to do the current piece of processing at the NIO layer for the response.
     * @return the total time taken in ms to process the response at the NIO layer, including the current piece, at this
     *          moment.
     */
    public long addToResponseProcessingTime(long delta) {
      return responseProcessingTimeInMs.addAndGet(delta);
    }

    /**
     * Marks the time at which the request was received.
     */
    public void markRequestReceived() {
      requestReceivedTime = System.currentTimeMillis();
    }

    /**
     * Marks the time at which the first byte of the response is sent.
     */
    public void markFirstByteSent() {
      if (requestReceivedTime == 0) {
        throw new IllegalStateException("First response byte was marked as sent without request being marked received");
      }
      timeToFirstByteInMs = System.currentTimeMillis() - requestReceivedTime;
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
   * Helper for updating scaling related metrics. These metrics are updated in the classes that provide scaling
   * capabilities when transferring control from {@link NioServer} to {@link RestRequestService}.
   */
  public static class ScalingMetricsTracker {
    private final AtomicLong requestProcessingTimeInMs = new AtomicLong(0);
    private final AtomicLong requestProcessingWaitTimeInMs = new AtomicLong(0);
    private final AtomicLong responseProcessingTimeInMs = new AtomicLong(0);
    private final AtomicLong responseProcessingWaitTimeInMs = new AtomicLong(0);

    private long requestReceivedTime = 0;
    private long roundTripTimeInMs = 0;

    /**
     * Adds to the time taken to process a request at the scaling layer.
     * @param delta the time taken in ms to do the current piece of processing at the scaling layer for the request.
     * @return the total time taken in ms to process this request at the scaling layer, including the current piece, at
     *          this moment.
     */
    public long addToRequestProcessingTime(long delta) {
      return requestProcessingTimeInMs.addAndGet(delta);
    }

    /**
     * Adds to the scaling layer processing wait time for a request.
     * @param delta the time in ms a request has spent waiting to be processed at the scaling layer.
     * @return the total time in ms this request has spent waiting to be processed at the scaling layer at this moment.
     */
    public long addToRequestProcessingWaitTime(long delta) {
      return requestProcessingWaitTimeInMs.addAndGet(delta);
    }

    /**
     * Adds to the time taken to process a response at the scaling layer.
     * @param delta the time taken in ms to do the current piece of processing at the scaling layer for the response.
     * @return the total time taken in ms to process the response at the scaling layer, including the current piece, at
     *          this moment.
     */
    public long addToResponseProcessingTime(long delta) {
      return responseProcessingTimeInMs.addAndGet(delta);
    }

    /**
     * Adds to the scaling layer processing wait time of a response.
     * @param delta the time in ms a response has spent waiting to be processed at the scaling layer.
     * @return the total time in ms this response has spent waiting to be processed at the scaling layer at this moment.
     */
    public long addToResponseProcessingWaitTime(long delta) {
      return responseProcessingWaitTimeInMs.addAndGet(delta);
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
   * Marks that the request is failed so that metrics can be tracked.
   */
  public void markFailure() {
    failed = true;
  }

  /**
   * Marks that the request is unsatisfied so that metrics can be tracked.
   */
  public void markUnsatisfied() {
    satisfied = false;
  }

  /**
   * Return whether the rest request is satisfied or not.
   * @return {@code true} if request is satisfied.
   */
  public boolean isSatisfied() {
    return satisfied;
  }

  /**
   * @param responseStatus the {@link ResponseStatus} to be used for certain count metrics.
   */
  public void setResponseStatus(ResponseStatus responseStatus) {
    this.responseStatus = responseStatus;
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

  public void injectContainerMetrics(ContainerMetrics containerMetrics) {
    this.containerMetrics = containerMetrics;
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
        metrics.operationRate.mark();
        metrics.operationCount.inc();
        metrics.nioRequestProcessingTimeInMs.update(nioMetricsTracker.requestProcessingTimeInMs.get());
        metrics.nioResponseProcessingTimeInMs.update(nioMetricsTracker.responseProcessingTimeInMs.get());
        metrics.nioRoundTripTimeInMs.update(nioMetricsTracker.roundTripTimeInMs);
        metrics.nioTimeToFirstByteInMs.update(nioMetricsTracker.timeToFirstByteInMs);

        metrics.scRequestProcessingTimeInMs.update(scalingMetricsTracker.requestProcessingTimeInMs.get());
        metrics.scRequestProcessingWaitTimeInMs.update(scalingMetricsTracker.requestProcessingWaitTimeInMs.get());
        metrics.scResponseProcessingTimeInMs.update(scalingMetricsTracker.responseProcessingTimeInMs.get());
        metrics.scResponseProcessingWaitTimeInMs.update(scalingMetricsTracker.responseProcessingWaitTimeInMs.get());
        metrics.scRoundTripTimeInMs.update(scalingMetricsTracker.roundTripTimeInMs);
        if (failed) {
          metrics.operationError.inc();
        }
        if (containerMetrics != null) {
          containerMetrics.recordMetrics(nioMetricsTracker.roundTripTimeInMs, responseStatus);
        }
        if (satisfied) {
          metrics.satisfiedRequestCount.inc();
        } else {
          metrics.unsatisfiedRequestCount.inc();
        }
      }
    } else {
      throw new IllegalStateException("Could not record metrics because there is no metrics tracker");
    }
  }

  /**
   * @return round trip time of this request in milliseconds
   */
  public long getRoundTripTimeInMs() {
    return nioMetricsTracker.roundTripTimeInMs;
  }

  /**
   * @return time to first byte of this request in milliseconds
   */
  public long getTimeToFirstByteInMs() {
    return nioMetricsTracker.timeToFirstByteInMs;
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
