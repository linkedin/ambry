package com.github.ambry.utils;

import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A class to measure and throttle the rate of some process. The throttler takes a desired rate-per-second
 * (the units of the process do not matter, it could be bytes or a count of some other thing), and will sleep for
 * an appropriate amount of time when maybeThrottle() is called to attain the desired rate.
 */
public class Throttler {

  private double desiredRatePerSec;
  private long checkIntervalMs;
  private boolean throttleDown;
  private Object lock = new Object();
  private Object waitGuard = new Object();
  private long periodStartNs;
  private double observedSoFar;
  private Logger logger = LoggerFactory.getLogger(getClass());
  private Time time;
  private AtomicBoolean enabled;

  /**
   * @param desiredRatePerSec: The rate we want to hit in units/sec
   * @param checkIntervalMs: The interval at which to check our rate
   * @param throttleDown: Does throttling increase or decrease our rate?
   * @param time: The time implementation to use
   **/
  public Throttler(double desiredRatePerSec, long checkIntervalMs, boolean throttleDown, Time time) {
    this.desiredRatePerSec = desiredRatePerSec;
    this.checkIntervalMs = checkIntervalMs;
    this.throttleDown = throttleDown;
    this.time = time;
    this.observedSoFar = 0.0;
    this.periodStartNs = time.nanoseconds();
    this.enabled = new AtomicBoolean(true);
  }

  /**
   * Throttle if required
   * @param observed the newly observed units since the last time this method was called.
   */
  public void maybeThrottle(double observed)
      throws InterruptedException {
    synchronized (lock) {
      observedSoFar += observed;
      long now = time.nanoseconds();
      long elapsedNs = now - periodStartNs;

      // if we have completed an interval AND we have observed something, maybe
      // we should take a little nap
      if (elapsedNs > checkIntervalMs * Time.NsPerMs && observedSoFar > 0) {
        double rateInSecs = (observedSoFar * Time.NsPerSec) / elapsedNs;
        boolean needAdjustment = !(throttleDown ^ (rateInSecs > desiredRatePerSec));
        if (needAdjustment) {
          // solve for the amount of time to sleep to make us hit the desired rate
          double desiredRateMs = desiredRatePerSec / Time.MsPerSec;
          double elapsedMs = elapsedNs / Time.NsPerMs;
          long sleepTime = Math.round(observedSoFar / desiredRateMs - elapsedMs);
          if (sleepTime > 0) {
            logger.trace("Natural rate is {} per second but desired rate is {}, sleeping for {} ms to compensate.",
                rateInSecs, desiredRatePerSec, sleepTime);
            synchronized (waitGuard) {
              // If an awake has already been called, do not wait.
              if (enabled.get()) {
                time.wait(waitGuard, sleepTime);
              }
            }
          }
        }
        periodStartNs = now;
        observedSoFar = 0;
      }
    }
  }

  /**
   * Awake the throttler if it is in the midst of throttling.
   */
  public void awake() {
    synchronized (waitGuard) {
      waitGuard.notify();
    }
  }

  /**
   * Enable the throttler. Note that this is the default state at creation time.
   */
  public void enable() {
    enabled.set(true);
  }

  /**
   * Disable the throttler. No throttling will be done in the calls to maybeThrottle() unless a call to enable() is made.
   */
  public void disable() {
    enabled.set(false);
  }
}