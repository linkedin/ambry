package com.github.ambry.store;

import java.util.ArrayDeque;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The Journal used by the index to optimize on finding the most recent
 * blobs written to the store
 */
public class BlobJournal {

  private final int maxSize;
  private AtomicInteger currentNumItems;
  private final ConcurrentSkipListMap<Long, MessageInfo> boundedQueue;

  public BlobJournal(int maxSize) {
    if (maxSize <= 0)
      throw new IllegalArgumentException("Max size needs to be larger than or equal to zero");
    this.maxSize = maxSize;
    this.boundedQueue = new ConcurrentSkipListMap<Long, MessageInfo>();
    this.currentNumItems = new AtomicInteger(0);
  }

  public void addToJournal(long offset, MessageInfo info) {
    if (info == null)
      throw new IllegalArgumentException("Message info cannot be null");
    if (maxSize == 0)
      return;
    if (!boundedQueue.isEmpty() && boundedQueue.lastKey() >= offset)
      throw new IllegalArgumentException("Cannot insert an offset lesser than the current " +
              "largest offset in journal largest " + boundedQueue.lastKey() +" new offset " + offset);
    if (currentNumItems.get() == maxSize) {
      boundedQueue.remove(boundedQueue.firstEntry());
      currentNumItems.decrementAndGet();
    }
    boundedQueue.put(offset, info);
    currentNumItems.incrementAndGet();
  }

  public ConcurrentNavigableMap<Long, MessageInfo> getEntriesAfter(long offset) {
    return boundedQueue.tailMap(offset, false);
  }
}
