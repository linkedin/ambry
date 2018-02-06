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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import java.util.Map;
import java.util.Random;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Unit tests for {@link RestRequestMetricsTracker}.
 */
public class RestRequestMetricsTrackerTest {

  /**
   * Tests the common case uses of {@link RestRequestMetricsTracker} i.e. with and without a custom
   * {@link RestRequestMetrics}.
   */
  @Test
  public void commonCaseTest() throws InterruptedException {
    withDefaultsTest(false);
    withDefaultsTest(true);
    withInjectedMetricsTest(false);
    withInjectedMetricsTest(true);
  }

  /**
   * Tests reaction of {@link RestRequestMetricsTracker#injectMetrics(RestRequestMetrics)} to bad input.
   */
  @Test
  public void injectMetricsBadInputTest() {
    RestRequestMetricsTracker requestMetrics = new RestRequestMetricsTracker();
    try {
      requestMetrics.injectMetrics(null);
      fail("There was no exception even though a null RestRequestMetrics was provided as input for injectMetrics()");
    } catch (IllegalArgumentException e) {
      // expected. nothing to do.
    }
  }

  /**
   * Tests reaction to bad calls to {@link RestRequestMetricsTracker.NioMetricsTracker#markRequestCompleted()} and
   * {@link RestRequestMetricsTracker.ScalingMetricsTracker#markRequestCompleted()}
   */
  @Test
  public void requestMarkingExceptionsTest() {
    RestRequestMetricsTracker requestMetrics = new RestRequestMetricsTracker();
    try {
      requestMetrics.nioMetricsTracker.markFirstByteSent();
      fail("Marking request as complete before marking it received should have thrown exception");
    } catch (IllegalStateException e) {
      // expected. nothing to do.
    }

    try {
      requestMetrics.nioMetricsTracker.markRequestCompleted();
      fail("Marking request as complete before marking it received should have thrown exception");
    } catch (IllegalStateException e) {
      // expected. nothing to do.
    }

    try {
      requestMetrics.scalingMetricsTracker.markRequestCompleted();
      fail("Marking request as complete before marking it received should have thrown exception");
    } catch (IllegalStateException e) {
      // expected. nothing to do.
    }
  }

  // commonCaseTest() helpers

  /**
   * Tests recording of metrics without setting a custom {@link RestRequestMetrics}.
   * @param induceFailure if {@code true}, the request is marked as failed.
   */
  private void withDefaultsTest(boolean induceFailure) throws InterruptedException {
    MetricRegistry metricRegistry = new MetricRegistry();
    RestRequestMetricsTracker.setDefaults(metricRegistry);
    RestRequestMetricsTracker requestMetrics = new RestRequestMetricsTracker();
    TestMetrics testMetrics = new TestMetrics(requestMetrics, induceFailure);
    requestMetrics.recordMetrics();
    String metricPrefix =
        RestRequestMetricsTracker.class.getCanonicalName() + "." + RestRequestMetricsTracker.DEFAULT_REQUEST_TYPE;
    testMetrics.compareMetrics(metricPrefix, metricRegistry);
  }

  /**
   * Tests recording of metrics after setting a custom {@link RestRequestMetrics}.
   * @param induceFailure if {@code true}, the request is marked as failed.
   */
  private void withInjectedMetricsTest(boolean induceFailure) throws InterruptedException {
    MetricRegistry metricRegistry = new MetricRegistry();
    RestRequestMetricsTracker.setDefaults(metricRegistry);
    String testRequestType = "Test";
    RestRequestMetricsTracker requestMetrics = new RestRequestMetricsTracker();
    RestRequestMetrics restRequestMetrics = new RestRequestMetrics(getClass(), testRequestType, metricRegistry);
    TestMetrics testMetrics = new TestMetrics(requestMetrics, induceFailure);
    requestMetrics.injectMetrics(restRequestMetrics);
    requestMetrics.recordMetrics();
    String metricPrefix = getClass().getCanonicalName() + "." + testRequestType;
    testMetrics.compareMetrics(metricPrefix, metricRegistry);
  }
}

/**
 * Class that randomly generates some metrics, updates them in the instance of {@link RestRequestMetricsTracker}
 * provided and then checks for equality once the metrics are recorded.
 */
class TestMetrics {
  private static final int REQUEST_SLEEP_TIME_MS = 5;
  private final Random random = new Random();
  private final long nioLayerRequestProcessingTime = random.nextInt(Integer.MAX_VALUE);
  private final long nioLayerResponseProcessingTime = random.nextInt(Integer.MAX_VALUE);
  private final long scRequestProcessingTime = random.nextInt(Integer.MAX_VALUE);
  private final long scResponseProcessingTime = random.nextInt(Integer.MAX_VALUE);
  private final long scRequestProcessingWaitTime = random.nextInt(Integer.MAX_VALUE);
  private final long scResponseProcessingWaitTime = random.nextInt(Integer.MAX_VALUE);

  private final long operationErrorCount;

  /**
   * Creates a new instance by generating new random metrics and updating it in the given {@code requestMetrics}.
   * @param requestMetrics the instance of {@link RestRequestMetricsTracker} where metrics have to be updated.
   * @param induceFailure if {@code true}, the request is marked as failed.
   */
  protected TestMetrics(RestRequestMetricsTracker requestMetrics, boolean induceFailure) throws InterruptedException {
    updateMetrics(requestMetrics, induceFailure);
    operationErrorCount = induceFailure ? 1 : 0;
  }

  /**
   * Compares metrics generated inside this instance with what was recorded in the given {@code metricRegistry}.
   * @param metricPrefix the prefix of the metrics to look for.
   * @param metricRegistry the {@link MetricRegistry} where metrics were recorded.
   */
  protected void compareMetrics(String metricPrefix, MetricRegistry metricRegistry) {
    Map<String, Histogram> histograms = metricRegistry.getHistograms();
    assertEquals("NIO request processing time unequal", nioLayerRequestProcessingTime,
        histograms.get(metricPrefix + RestRequestMetrics.NIO_REQUEST_PROCESSING_TIME_SUFFIX)
            .getSnapshot()
            .getValues()[0]);
    assertEquals("NIO response processing time unequal", nioLayerResponseProcessingTime,
        histograms.get(metricPrefix + RestRequestMetrics.NIO_RESPONSE_PROCESSING_TIME_SUFFIX)
            .getSnapshot()
            .getValues()[0]);

    long timeToFirstByte =
        histograms.get(metricPrefix + RestRequestMetrics.NIO_TIME_TO_FIRST_BYTE_SUFFIX).getSnapshot().getValues()[0];
    assertTrue("NIO time to first byte " + timeToFirstByte + "<" + REQUEST_SLEEP_TIME_MS,
        timeToFirstByte >= REQUEST_SLEEP_TIME_MS);
    long roundTripTime =
        histograms.get(metricPrefix + RestRequestMetrics.NIO_ROUND_TRIP_TIME_SUFFIX).getSnapshot().getValues()[0];
    assertTrue("NIO round trip time " + roundTripTime + "<" + REQUEST_SLEEP_TIME_MS * 2,
        roundTripTime >= REQUEST_SLEEP_TIME_MS * 2);

    assertEquals("SC request processing time unequal", scRequestProcessingTime,
        histograms.get(metricPrefix + RestRequestMetrics.SC_REQUEST_PROCESSING_TIME_SUFFIX)
            .getSnapshot()
            .getValues()[0]);
    assertEquals("SC request processing wait time unequal", scRequestProcessingWaitTime,
        histograms.get(metricPrefix + RestRequestMetrics.SC_REQUEST_PROCESSING_WAIT_TIME_SUFFIX)
            .getSnapshot()
            .getValues()[0]);
    assertEquals("SC response processing time unequal", scResponseProcessingTime,
        histograms.get(metricPrefix + RestRequestMetrics.SC_RESPONSE_PROCESSING_TIME_SUFFIX)
            .getSnapshot()
            .getValues()[0]);
    assertEquals("SC response processing wait time unequal", scResponseProcessingWaitTime,
        histograms.get(metricPrefix + RestRequestMetrics.SC_RESPONSE_PROCESSING_WAIT_TIME_SUFFIX)
            .getSnapshot()
            .getValues()[0]);

    assertEquals("Rate metric has not fired", 1,
        metricRegistry.getMeters().get(metricPrefix + RestRequestMetrics.OPERATION_RATE_SUFFIX).getCount());
    assertEquals("Error metric value is not as expected", operationErrorCount,
        metricRegistry.getCounters().get(metricPrefix + RestRequestMetrics.OPERATION_ERROR_SUFFIX).getCount());
  }

  /**
   * Updates the generated metrics in the given {@code restRequestMetricsTracker}.
   * @param restRequestMetricsTracker the instance of {@link RestRequestMetricsTracker} where metrics have to be
   *                                  updated.
   * @param induceFailure if {@code true}, the request is marked as failed.
   */
  private void updateMetrics(RestRequestMetricsTracker restRequestMetricsTracker, boolean induceFailure)
      throws InterruptedException {
    restRequestMetricsTracker.nioMetricsTracker.addToRequestProcessingTime(nioLayerRequestProcessingTime);
    restRequestMetricsTracker.nioMetricsTracker.addToResponseProcessingTime(nioLayerResponseProcessingTime);

    restRequestMetricsTracker.nioMetricsTracker.markRequestReceived();
    Thread.sleep(REQUEST_SLEEP_TIME_MS);
    restRequestMetricsTracker.nioMetricsTracker.markFirstByteSent();
    Thread.sleep(REQUEST_SLEEP_TIME_MS);
    restRequestMetricsTracker.nioMetricsTracker.markRequestCompleted();

    restRequestMetricsTracker.scalingMetricsTracker.addToRequestProcessingTime(scRequestProcessingTime);
    restRequestMetricsTracker.scalingMetricsTracker.addToResponseProcessingTime(scResponseProcessingTime);
    restRequestMetricsTracker.scalingMetricsTracker.addToRequestProcessingWaitTime(scRequestProcessingWaitTime);
    restRequestMetricsTracker.scalingMetricsTracker.addToResponseProcessingWaitTime(scResponseProcessingWaitTime);

    if (induceFailure) {
      restRequestMetricsTracker.markFailure();
    }
  }
}
