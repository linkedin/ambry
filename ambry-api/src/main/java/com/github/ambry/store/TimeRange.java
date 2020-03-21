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

/**
 * A TimeRange is the range between two times in milliseconds
 */
public class TimeRange {

  private final long startTimeInMs;
  private final long endTimeInMs;

  /**
   * Instantiates a {@link TimeRange} referring to a reference time with an allowed error margin
   * @param referenceTimeInMs the reference time in milliseconds that this {@link TimeRange} is referring to
   * @param errorMarginInMs the allowable error margin in milliseconds
   */
  public TimeRange(long referenceTimeInMs, long errorMarginInMs) {
    if (errorMarginInMs < 0 || referenceTimeInMs < 0 || referenceTimeInMs - errorMarginInMs < 0
        || referenceTimeInMs > Long.MAX_VALUE - errorMarginInMs) {
      throw new IllegalArgumentException(
          "Illegal reference time: " + referenceTimeInMs + " and/or error margin: " + errorMarginInMs);
    }
    startTimeInMs = referenceTimeInMs - errorMarginInMs;
    endTimeInMs = referenceTimeInMs + errorMarginInMs;
  }

  /**
   * @return the start time in Secs that this {@link TimeRange} is referring to
   */
  public long getStartTimeInMs() {
    return startTimeInMs;
  }

  /**
   * @return the end time in Secs that this {@link TimeRange} is referring to
   */
  public long getEndTimeInMs() {
    return endTimeInMs;
  }
}
