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
import com.github.ambry.frontend.ContainerMetrics;
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
   * Tests that {@link RestRequestMetricsTracker#markServerError()} increments a metric that is separate and
   * independent from the unsatisfied/satisfied request count metrics tracked via
   * {@link RestRequestMetricsTracker#markUnsatisfied()}.
   */
  @Test
  public void markServerErrorTest() {
    // server error marked, request otherwise satisfied (mirrors requests unsatisfied only due to a 5xx response).
    serverErrorTest(true, true);
    // server error marked and request also marked unsatisfied (mirrors requests unsatisfied due to 5xx AND missed
    // thresholds).
    serverErrorTest(true, false);
    // no server error, request satisfied.
    serverErrorTest(false, true);
    // no server error, request unsatisfied (e.g. missed thresholds on a non-5xx response).
    serverErrorTest(false, false);
  }

  /**
   * Tests recording of the server error metric in combination with the satisfied/unsatisfied request metrics.
   * @param induceServerError if {@code true}, {@link RestRequestMetricsTracker#markServerError()} is called.
   * @param satisfied if {@code true}, the request is left in its default satisfied state; if {@code false},
   *                  {@link RestRequestMetricsTracker#markUnsatisfied()} is called.
   */
  private void serverErrorTest(boolean induceServerError, boolean satisfied) {
    MetricRegistry metricRegistry = new MetricRegistry();
    RestRequestMetricsTracker.setDefaults(metricRegistry);
    String testRequestType = "ServerErrorTest";
    RestRequestMetricsTracker requestMetrics = new RestRequestMetricsTracker();
    RestRequestMetrics restRequestMetrics = new RestRequestMetrics(getClass(), testRequestType, metricRegistry);
    requestMetrics.injectMetrics(restRequestMetrics);

    assertFalse("Request should not be a server error by default", requestMetrics.isServerError());
    if (induceServerError) {
      requestMetrics.markServerError();
    }
    if (!satisfied) {
      requestMetrics.markUnsatisfied();
    }
    assertEquals("isServerError() does not reflect markServerError() call", induceServerError,
        requestMetrics.isServerError());
    assertEquals("isSatisfied() should be unaffected by markServerError()", satisfied, requestMetrics.isSatisfied());

    requestMetrics.recordMetrics();

    String metricPrefix = getClass().getCanonicalName() + "." + testRequestType;
    long expectedServerErrorCount = induceServerError ? 1 : 0;
    assertEquals("Server error count metric value is not as expected", expectedServerErrorCount,
        metricRegistry.getCounters().get(metricPrefix + RestRequestMetrics.SERVER_ERROR_COUNT_SUFFIX).getCount());
    assertEquals("Satisfied request count metric value is not as expected", satisfied ? 1 : 0,
        metricRegistry.getCounters().get(metricPrefix + RestRequestMetrics.SATISFIED_REQUEST_COUNT_SUFFIX).getCount());
    assertEquals("Unsatisfied request count metric value is not as expected", satisfied ? 0 : 1,
        metricRegistry.getCounters()
            .get(metricPrefix + RestRequestMetrics.UNSATISFIED_REQUEST_COUNT_SUFFIX)
            .getCount());
  }

  /**
   * Tests that a client abort is counted separately from the {@link ResponseStatus#BadRequest} it is reported as,
   * without changing the value of any existing per-container series.
   */
  @Test
  public void clientAbortCountedSeparatelyFromBadRequestTest() {
    clientAbortTest(true);
    clientAbortTest(false);
  }

  /**
   * Records a {@link ResponseStatus#BadRequest} against a container, optionally marking it as a client abort, and
   * checks the resulting counters.
   * @param clientAborted if {@code true}, {@link RestRequestMetricsTracker#markClientAborted()} is called.
   */
  private void clientAbortTest(boolean clientAborted) {
    MetricRegistry metricRegistry = new MetricRegistry();
    RestRequestMetricsTracker.setDefaults(metricRegistry);
    RestRequestMetricsTracker requestMetrics = new RestRequestMetricsTracker();
    requestMetrics.injectMetrics(new RestRequestMetrics(getClass(), "ClientAbortTest", metricRegistry));
    requestMetrics.injectContainerMetrics(
        new ContainerMetrics("account", "container", "PostBlob", metricRegistry, false, null));
    // A client termination is reported to the client as 400, so this is the status an aborted request arrives with.
    requestMetrics.setResponseStatus(ResponseStatus.BadRequest);
    if (clientAborted) {
      requestMetrics.markClientAborted();
    }

    requestMetrics.recordMetrics();

    String metricPrefix = ContainerMetrics.class.getCanonicalName() + ".account___container___PostBlob";
    assertEquals("Client abort count is not as expected", clientAborted ? 1 : 0,
        metricRegistry.getCounters().get(metricPrefix + "ClientAbortCount").getCount());
    // The point of the separate counter is that these two keep their existing meaning, so that no dashboard built on
    // them changes value when this ships. Aborts are subtracted out using ClientAbortCount instead.
    assertEquals("Aborts must keep counting towards the bad request count", 1,
        metricRegistry.getCounters().get(metricPrefix + "BadRequestCount").getCount());
    assertEquals("Aborts must keep counting towards the client error count", 1,
        metricRegistry.getCounters().get(metricPrefix + "ClientErrorCount").getCount());
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

  /**
   * Tests {@link RestRequestMetricsTracker#getTimeSinceRequestReceivedInMs()}. Unlike
   * {@link RestRequestMetricsTracker.NioMetricsTracker#markFirstByteSent()} and
   * {@link RestRequestMetricsTracker.NioMetricsTracker#markRequestCompleted()}, it returns {@code 0} rather than
   * throwing when the request was never marked as received, because its callers are diagnostic paths that must not be
   * turned into failures.
   */
  @Test
  public void testTimeSinceRequestReceived() {
    RestRequestMetricsTracker requestMetrics = new RestRequestMetricsTracker();
    assertEquals("Time since request received should be 0 when the request was never marked received", 0,
        requestMetrics.getTimeSinceRequestReceivedInMs());

    long beforeMs = System.currentTimeMillis();
    requestMetrics.nioMetricsTracker.markRequestReceived();
    // Busy wait so that the elapsed time is provably non-zero. Thread.sleep() is not used for test synchronization.
    long deadlineMs = System.currentTimeMillis() + 2;
    while (System.currentTimeMillis() < deadlineMs) {
      Thread.yield();
    }

    long timeSinceReceivedMs = requestMetrics.getTimeSinceRequestReceivedInMs();
    assertTrue("Time since request received " + timeSinceReceivedMs + " ms should have advanced past 0",
        timeSinceReceivedMs > 0);
    // An elapsed time, unlike a wall clock timestamp, cannot exceed the time this test has been running for.
    assertTrue("Time since request received " + timeSinceReceivedMs + " ms should not exceed the elapsed test time",
        timeSinceReceivedMs <= System.currentTimeMillis() - beforeMs);
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
    testMetrics.compareMetrics(metricPrefix, metricRegistry, induceFailure);
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
    testMetrics.compareMetrics(metricPrefix, metricRegistry, induceFailure);
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

  private final long bytesTransferred = 12345678;

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
  protected void compareMetrics(String metricPrefix, MetricRegistry metricRegistry, boolean failed) {
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

    if (failed) {
      assertTrue("Expected throughput to not have been calculated when request failed",
          histograms.get(metricPrefix + RestRequestMetrics.THROUGHPUT_SUFFIX).getSnapshot().getValues().length == 0);
    } else {
      assertTrue("Expected throughput to have been calculated when request succeeded",
          histograms.get(metricPrefix + RestRequestMetrics.THROUGHPUT_SUFFIX).getSnapshot().getValues()[0] > 0);
    }
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

    restRequestMetricsTracker.setBytesTransferred(bytesTransferred);

    if (induceFailure) {
      restRequestMetricsTracker.markFailure();
    }
  }
}
