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
import org.junit.Test;

import static org.junit.Assert.*;


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
