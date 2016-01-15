package com.github.ambry.utils;

import org.junit.Assert;
import org.junit.Test;


/**
 * Mocks time and tests the throttler code
 */
public class ThrottlerTest {
  @Test
  public void throttlerTest()
      throws InterruptedException {
    MockThrottlerTime time = new MockThrottlerTime();
    time.currentMilliseconds = 0;
    time.currentNanoSeconds = 0;
    time.sleepTimeExpected = 0;
    Throttler throttler = new Throttler(100, 10, true, time);
    throttler.maybeThrottle(50);
    Assert.assertEquals(time.currentMilliseconds, 0);
    time.currentNanoSeconds = 11 * Time.NsPerMs;
    time.currentMilliseconds = 11;
    time.sleepTimeExpected = 1489;
    throttler.maybeThrottle(100);
    time.currentNanoSeconds = 22 * Time.NsPerMs;
    time.currentMilliseconds = 22;
    time.sleepTimeExpected = 4989;
    throttler.maybeThrottle(500);
    Assert.assertEquals(time.currentMilliseconds, 5011);
  }

  class MockThrottlerTime extends MockTime {
    long sleepTimeExpected;

    @Override
    public void sleep(long ms)
        throws InterruptedException {
      currentMilliseconds += ms;
      if (sleepTimeExpected != ms) {
        throw new IllegalArgumentException();
      }
    }
  }
}
