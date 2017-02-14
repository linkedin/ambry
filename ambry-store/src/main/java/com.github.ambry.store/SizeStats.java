package com.github.ambry.store;

public class SizeStats {
  private long totalSize;
  private long aggregatedCache;
  private long lastAggregatedBucket;

  public long getTotalSize() {
    return totalSize;
  }

  public void setTotalSize(long totalSize) {
    this.totalSize = totalSize;
  }

  public long getCachedDelta() {
    return aggregatedCache;
  }

  public void setCachedDelta(long cachedDelta) {
    this.aggregatedCache = cachedDelta;
  }

  public long getLastAggregatedBucket() {
    return lastAggregatedBucket;
  }

  public void setLastAggregatedBucket(long lastAggregatedBucket) {
    this.lastAggregatedBucket = lastAggregatedBucket;
  }
}
