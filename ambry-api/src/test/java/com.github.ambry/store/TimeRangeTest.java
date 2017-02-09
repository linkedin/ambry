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


public class TimeRangeTest {

  @Test
  public void testTimeRange() {
    long currentTime = System.currentTimeMillis();
    long[] startTimes =
        new long[]{currentTime, currentTime - 10000, currentTime - 1, currentTime + 1, currentTime + 10000};
    long[] errorMargins = new long[]{0, 1, 1000};
    for (long startTime : startTimes) {
      for (long errorMargin : errorMargins) {
        verifyEquality(new TimeRange(startTime, errorMargin), startTime - errorMargin, startTime + errorMargin);
      }
    }
  }

  @Test
  public void testTimeRangeFailure() {
    long[] startTimes =
        new long[]{-1000, -1};
    long[] errorMargins = new long[]{-1000, -1};
    for (long startTime : startTimes) {
      for (long errorMargin : errorMargins) {
        verifyConstructionFailure(startTime, errorMargin);
      }
    }
  }

  /**
   * Verifies that the {@link TimeRange} has the expected start time and end time
   * @param timeRange the {@link TimeRange} that needs to be verified
   * @param startTime the start time that the {@link TimeRange} is expected to refer to
   * @param endTime the end time that the {@link TimeRange} is expected to refer to
   */
  private void verifyEquality(TimeRange timeRange, long startTime, long endTime) {
    assertEquals("StartTime didn't match", startTime, timeRange.getStart());
    assertEquals("EndTime didn't match", endTime, timeRange.getEnd());
  }

  /**
   * Verifies that the {@link TimeRange} instantitation fails for the given args
   * @param referenceTime the reference time that is to be used to construct the {@link TimeRange}
   * @param errorMargin the error margin that is to be used to construct the {@link TimeRange}
   */
  private void verifyConstructionFailure(long referenceTime, long errorMargin) {
    try{
      new TimeRange(referenceTime, errorMargin);
    } catch (IllegalArgumentException e){
    }
  }
}
