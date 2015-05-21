package com.github.ambry.store;

public class FindEntriesCondition {
  private long endTime;
  private long maxSize;

  FindEntriesCondition(long maxSize) {
    this.maxSize = maxSize;
    this.endTime = -1;
  }

  FindEntriesCondition(long maxSize, long endTime) {
    this.maxSize = maxSize;
    this.endTime = endTime;
  }

  boolean proceed(long checkSize, long checkEndTime) {
    return (endTime == -1 || endTime >= checkEndTime) && (maxSize > checkSize);
  }
}

