package com.github.ambry.rest;

import com.codahale.metrics.MetricRegistry;
import org.junit.Test;

import static org.junit.Assert.fail;


/**
 * Unit tests for {@link RestRequestMetrics}.
 */
public class RestRequestMetricsTest {

  /**
   * Tests instantiation of {@link RestRequestMetrics} with bad input.
   */
  @Test
  public void badInstantiationTest() {
    Class ownerClass = getClass();
    String requestType = "Test";
    MetricRegistry metricRegistry = new MetricRegistry();

    // ownerClass null
    try {
      new RestRequestMetrics(null, requestType, metricRegistry);
      fail("There was no exception even though one of the instantiation arguments for RestRequestMetrics is null");
    } catch (IllegalArgumentException e) {
      // expected. nothing to do.
    }

    // requestType null
    try {
      new RestRequestMetrics(ownerClass, null, metricRegistry);
      fail("There was no exception even though one of the instantiation arguments for RestRequestMetrics is null");
    } catch (IllegalArgumentException e) {
      // expected. nothing to do.
    }

    // MetricRegistry null
    try {
      new RestRequestMetrics(ownerClass, requestType, null);
      fail("There was no exception even though one of the instantiation arguments for RestRequestMetrics is null");
    } catch (IllegalArgumentException e) {
      // expected. nothing to do.
    }
  }
}
