package com.github.ambry.store;

/**
 * A TimeRange is the range between two times in Ms
 */
class TimeRange {

  private final long startTimeInMs;
  private final long endTimeInMs;

  /**
   * Instantiates a {@link TimeRange} referring to a reference time with an allowed error margin
   * @param referenceTimeInMs the reference time in Ms that this {@link TimeRange} is referring to
   * @param errorMarginInMs the allowable error margin in Ms
   */
  TimeRange(long referenceTimeInMs, long errorMarginInMs) {
    if(referenceTimeInMs < 0 || errorMarginInMs < 0){
      throw new IllegalArgumentException("Reference time "+referenceTimeInMs+" or Error margin "+errorMarginInMs+" cannot be negative ");
    }
    this.startTimeInMs = referenceTimeInMs - errorMarginInMs;
    this.endTimeInMs = referenceTimeInMs + errorMarginInMs;
  }

  /**
   * @return the start time in Ms that this {@link TimeRange} is referring to
   */
  public long getStart() {
    return startTimeInMs;
  }

  /**
   * @return the end time in Ms that this {@link TimeRange} is referring to
   */
  public long getEnd() {
    return endTimeInMs;
  }
}