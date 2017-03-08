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

import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests for the {@link TimeRange} class.
 */
public class TimeRangeTest {

  /**
   * Tests for the constructor and getters of TimeRange.
   */
  @Test
  public void testTimeRange() {
    long referenceTimeInSecs = 10L;
    long errorMarginInSecs = 5L;
    TimeRange timeRangeWithNoErrorMargin = new TimeRange(referenceTimeInSecs, 0L);
    TimeRange timeRangeWithErrorMargin = new TimeRange(referenceTimeInSecs, errorMarginInSecs);
    assertEquals("Start time and end time should be equal for TimeRange with no error margin",
        timeRangeWithNoErrorMargin.getStartTimeInSecs(), timeRangeWithNoErrorMargin.getEndTimeInSecs());
    assertEquals("TimeRange with no error margin is constructed incorrectly", referenceTimeInSecs,
        timeRangeWithNoErrorMargin.getStartTimeInSecs());
    assertEquals("Start time of TimeRange with error margin is incorrect", referenceTimeInSecs - errorMarginInSecs,
        timeRangeWithErrorMargin.getStartTimeInSecs());
    assertEquals("End time of TimeRange with error margin is incorrect", referenceTimeInSecs + errorMarginInSecs,
        timeRangeWithErrorMargin.getEndTimeInSecs());
  }

  /**
   * Tests to ensure IllegalArgumentException is thrown when given illegal arguments.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testTimeRangeIllegalArguments() {
    // construct a TimeRange with illegal arguments
    TimeRange timeRange = new TimeRange(-1, -1);
  }
}
