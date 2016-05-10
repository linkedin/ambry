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
package com.github.ambry.metrics;

import org.junit.Assert;
import org.junit.Test;


public class TestNoOpMetricsRegistry {

  @Test
  public void testNoOpMetricsHappyPath() {
    NoOpMetricsRegistry registry = new NoOpMetricsRegistry();
    Counter counter1 = registry.newCounter("testc", "a");
    Counter counter2 = registry.newCounter("testc", "b");
    Counter counter3 = registry.newCounter("testc2", "c");
    Gauge<String> gauge1 = registry.newGauge("testg", "a", "1");
    Gauge<String> gauge2 = registry.newGauge("testg", "b", "2");
    Gauge<String> gauge3 = registry.newGauge("testg", "c", "3");
    Gauge<String> gauge4 = registry.newGauge("testg2", "d", "4");
    counter1.inc();
    counter2.inc(2);
    counter3.inc(4);
    gauge1.set("5");
    gauge2.set("6");
    gauge3.set("7");
    gauge4.set("8");
    Assert.assertEquals(counter1.getCount(), 1);
    Assert.assertEquals(counter2.getCount(), 2);
    Assert.assertEquals(counter3.getCount(), 4);
    Assert.assertEquals(gauge1.getValue(), "5");
    Assert.assertEquals(gauge2.getValue(), "6");
    Assert.assertEquals(gauge3.getValue(), "7");
    Assert.assertEquals(gauge4.getValue(), "8");
  }
}
