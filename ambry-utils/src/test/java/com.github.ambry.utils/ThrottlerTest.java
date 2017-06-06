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

import org.junit.Assert;
import org.junit.Test;


/**
 * Tests {@link Throttler}.
 */
public class ThrottlerTest {
  private final MockTime time = new MockTime();

  /**
   * Basic test
   * @throws InterruptedException
   */
  @Test
  public void throttlerTest() throws InterruptedException {
    time.setCurrentMilliseconds(0);
    Throttler throttler = new Throttler(100, 10, true, time);
    throttler.maybeThrottle(0);
    Assert.assertEquals(0, time.milliseconds());
    throttler.maybeThrottle(50);
    Assert.assertEquals(0, time.milliseconds());
    time.setCurrentMilliseconds(11);
    throttler.maybeThrottle(100);
    Assert.assertEquals(1500, time.milliseconds());
    throttler.maybeThrottle(500);
    Assert.assertEquals(5011, time.milliseconds());
    throttler.disable();
    // no more sleeps after disable
    throttler.maybeThrottle(500);
    Assert.assertEquals(5011, time.milliseconds());
  }

  /**
   * Tests for different check intervals - positive, zero and check every time.
   * @throws InterruptedException
   */
  @Test
  public void checkIntervalTest() throws InterruptedException {
    time.setCurrentMilliseconds(0);
    // positive check interval
    Throttler throttler = new Throttler(100, 10, true, time);
    throttler.maybeThrottle(150);
    // no throttling because checkInterval has not elapsed
    Assert.assertEquals(0, time.milliseconds());
    time.setCurrentMilliseconds(11);
    throttler.maybeThrottle(0);
    Assert.assertEquals(1500, time.milliseconds());

    // zero check interval
    time.setCurrentMilliseconds(0);
    throttler = new Throttler(100, 0, true, time);
    throttler.maybeThrottle(130);
    // no throttling because checkInterval has not elapsed
    Assert.assertEquals(0, time.milliseconds());
    time.setCurrentNanoSeconds(10);
    throttler.maybeThrottle(20);
    Assert.assertEquals(1500, time.milliseconds());

    // no check interval, check every time
    time.setCurrentMilliseconds(0);
    throttler = new Throttler(100, -1, true, time);
    throttler.maybeThrottle(0);
    // no throttling because no work done
    Assert.assertEquals(0, time.milliseconds());
    throttler.maybeThrottle(150);
    Assert.assertEquals(1500, time.milliseconds());
  }
}
