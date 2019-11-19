package com.github.ambry.utils;

import com.codahale.metrics.Clock;
import java.util.concurrent.TimeUnit;


public class MockClock extends Clock {
  private long tick = 0;

  @Override
  public long getTick() {
    return tick;
  }

  public void tick(int seconds) {
    // Meter's TICK_INTERVAL = TimeUnit.SECONDS.toNanos(5)
    tick += TimeUnit.SECONDS.toNanos(seconds) + 1;
  }
}
