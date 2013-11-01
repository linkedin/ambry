package com.github.ambry.utils;

/**
 * The normal system implementation of time functions
 */
public class SystemTime extends Time {

  private static SystemTime time = new SystemTime();

  public static Time getInstance() {
    return time;
  }

  private SystemTime() {
  }

  public long milliseconds() {
    return System.currentTimeMillis();
  }

  public long nanoseconds() {
    return System.nanoTime();
  }

  public void sleep(long ms) throws InterruptedException {
    Thread.sleep(ms);
  }
}
