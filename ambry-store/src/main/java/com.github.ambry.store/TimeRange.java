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
 * A TimeRange is the range between two times in Ms
 */
class TimeRange {

  private final long startTimeInSecs;
  private final long endTimeInSecs;

  /**
   * Instantiates a {@link TimeRange} referring to a reference time with an allowed error margin
   * @param referenceTimeInSecs the reference time in Secs that this {@link TimeRange} is referring to
   * @param errorMarginInSecs the allowable error margin in Secs
   */
  TimeRange(long referenceTimeInSecs, long errorMarginInSecs) {
    if (referenceTimeInSecs < 0 || errorMarginInSecs < 0) {
      throw new IllegalArgumentException(
          "Reference time " + referenceTimeInSecs + " or Error margin " + errorMarginInSecs + " cannot be negative ");
    }
    startTimeInSecs = referenceTimeInSecs - errorMarginInSecs;
    endTimeInSecs = referenceTimeInSecs + errorMarginInSecs;
  }

  /**
   * @return the start time in Secs that this {@link TimeRange} is referring to
   */
  long getStartTimeInSecs() {
    return startTimeInSecs;
  }

  /**
   * @return the end time in Secs that this {@link TimeRange} is referring to
   */
  long getEndTimeInSecs() {
    return endTimeInSecs;
  }
}