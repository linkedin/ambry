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


/**
 * A class to measure and throttle the rate.
 */
public class RejectThrottler {
  private final double desiredRatePerSec;
  private final Meter meter;

  /**
   * @param desiredRatePerSec: The rate we want to hit in units/sec.
   * @param meter: internal meter used to track rate.
   */
  public RejectThrottler(long desiredRatePerSec, Meter meter) {
    this.desiredRatePerSec = desiredRatePerSec;
    this.meter = meter;
  }

  /**
   * @param desiredRatePerSec: The rate we want to hit in units/sec.
   */
  public RejectThrottler(long desiredRatePerSec) {
    this(desiredRatePerSec, new Meter());
  }

  /**
   * Return True if throttle is required.
   * @param observed the newly observed units since the last time this method was called.
   */
  public boolean shouldThrottle(long observed) {
    if (desiredRatePerSec < 0) {
      return false;
    }
    if (meter.getOneMinuteRate() >= desiredRatePerSec) {
      return true;
    } else {
      meter.mark(observed);
      return false;
    }
  }
}
