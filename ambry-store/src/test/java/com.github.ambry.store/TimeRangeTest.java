/**
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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

package com.github.ambry.store;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.*;


/**
 * Tests for the {@link TimeRange} class.
 */
public class TimeRangeTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  /**
   * Tests for the constructor and getters of TimeRange.
   */
  @Test
  public void testTimeRange() {
    long referenceTimeInMs = 10000L;
    long errorMarginInMs = 500L;
    TimeRange timeRangeWithNoErrorMargin = new TimeRange(referenceTimeInMs, 0L);
    TimeRange timeRangeWithErrorMargin = new TimeRange(referenceTimeInMs, errorMarginInMs);
    assertEquals("Start time and end time should be equal for TimeRange with no error margin",
        timeRangeWithNoErrorMargin.getStartTimeInMs(), timeRangeWithNoErrorMargin.getEndTimeInMs());
    assertEquals("TimeRange with no error margin is constructed incorrectly", referenceTimeInMs,
        timeRangeWithNoErrorMargin.getStartTimeInMs());
    assertEquals("Start time of TimeRange with error margin is incorrect", referenceTimeInMs - errorMarginInMs,
        timeRangeWithErrorMargin.getStartTimeInMs());
    assertEquals("End time of TimeRange with error margin is incorrect", referenceTimeInMs + errorMarginInMs,
        timeRangeWithErrorMargin.getEndTimeInMs());
  }

  /**
   * Tests to ensure IllegalArgumentException is thrown when given negative arguments.
   */
  @Test
  public void testExceptionIsThrownWithNegativeArguments() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("cannot be negative");
    TimeRange timeRange = new TimeRange(-1, -1);
  }

  /**
   * Tests to ensure IllegalArgumentException is thrown when given reference time minus error margin is negative.
   */
  @Test
  public void testExceptionIsThrownWithNegativeStartTime() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("cannot be negative");
    TimeRange timeRange = new TimeRange(1000, 10000);
  }

  /**
   * Tests to ensure IllegalArgumentException is thrown when given arguments will cause overflow.
   */
  @Test
  public void testExceptionIsThrownWithOverflowArguments() {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("should not overflow");
    TimeRange timeRange = new TimeRange(Long.MAX_VALUE, 1000);
  }
}
