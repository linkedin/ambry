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
package com.github.ambry.utils;

import com.codahale.metrics.Meter;
import org.junit.Assert;
import org.junit.Test;


/**
 * Tests {@link Throttler}.
 */
public class RejectThrottlerTest {

  /**
   * Basic test
   * @throws InterruptedException
   */
  @Test
  public void basicTest() throws Exception {
    // desired rate = -1: allows all request
    RejectThrottler rejectThrottler = new RejectThrottler(-1, null);
    Assert.assertFalse(rejectThrottler.shouldThrottle(-1));
    Assert.assertFalse(rejectThrottler.shouldThrottle(0));
    Assert.assertFalse(rejectThrottler.shouldThrottle(1));
    Assert.assertFalse(rejectThrottler.shouldThrottle(100));

    // desired rate = 0: denies all requests
    MockClock mockClock = new MockClock();
    Meter testMeter = new Meter(mockClock);
    rejectThrottler = new RejectThrottler(0, testMeter);
    mockClock.tick(5);
    Assert.assertTrue(rejectThrottler.shouldThrottle(0));
    Assert.assertTrue(rejectThrottler.shouldThrottle(1));
    Assert.assertTrue(rejectThrottler.shouldThrottle(21));

    // desire rate = 20: deny if rate >= 20, allow if rate < 20
    mockClock = new MockClock();
    testMeter = new Meter(mockClock);
    rejectThrottler = new RejectThrottler(20, testMeter);
    Assert.assertFalse(rejectThrottler.shouldThrottle(100));
    mockClock.tick(5);
    Assert.assertEquals("Rate should be 20", 20, testMeter.getOneMinuteRate(), 0.1);
    Assert.assertTrue("No more requests allowed.", rejectThrottler.shouldThrottle(1));
    mockClock.tick(5);
    Assert.assertTrue("Rate should less than 20. New requests are welcomed.", testMeter.getOneMinuteRate() < 20);
    Assert.assertFalse(rejectThrottler.shouldThrottle(1));
    Assert.assertFalse(rejectThrottler.shouldThrottle(200));
    mockClock.tick(5);
    Assert.assertTrue("Rate should greater than or equal to 20", testMeter.getOneMinuteRate() >= 20);
    Assert.assertTrue("No more requests.", rejectThrottler.shouldThrottle(1));
  }
}
