package com.github.ambry.utils;

/**
 * A mock time class
 */
public class MockTime extends Time {
  public long currentMilliseconds;
  public long currentNanoSeconds;

  public MockTime(long initialMilliseconds) {
    currentMilliseconds = initialMilliseconds;
    currentNanoSeconds = initialMilliseconds * NsPerMs;
  }

  public MockTime() {
    this(0);
  }

  @Override
  public long milliseconds() {
    return currentMilliseconds;
  }

  @Override
  public long nanoseconds() {
    return currentNanoSeconds;
  }

  @Override
  public long seconds() {
    return currentMilliseconds/MsPerSec;
  }

  @Override
  public void sleep(long ms)
      throws InterruptedException {
    currentMilliseconds += ms;
  }

  @Override
  public void wait(Object o, long ms)
    throws InterruptedException {
    sleep(ms);
  }
}
