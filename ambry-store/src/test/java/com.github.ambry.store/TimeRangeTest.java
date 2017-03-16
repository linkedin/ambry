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

import java.util.Random;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests for the {@link TimeRange} class.
 */
public class TimeRangeTest {

  /**
   * Tests the constructor and getters of TimeRange when provided with legal arguments.
   */
  @Test
  public void testTimeRange() {
    Random rand = new Random();
    long referenceTimeInMs = rand.nextInt(10000) + 10000;
    TimeRange timeRangeWithNoErrorMargin = new TimeRange(referenceTimeInMs, 0L);
    long errorMarginInMs = rand.nextInt(10000);
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
   * Tests to ensure IllegalArgumentException is thrown when given illegal arguments.
   */
  @Test
  public void testIllegalConstructionForTimeRange() {
    long[][] illegalArgs = new long[][]{{0, -1}, {0, 1}, {-1, 0}, {-1, -1}, {Long.MAX_VALUE, 1}, {1, Long.MAX_VALUE}};
    for (int i = 0; i < illegalArgs.length; i++) {
      try {
        new TimeRange(illegalArgs[i][0], illegalArgs[i][1]);
        fail("Expected IllegalArgumentException not thrown when constructing TimeRange with illegal arguments");
      } catch (IllegalArgumentException e) {
        // Expected exception thrown
      }
    }
  }
}
