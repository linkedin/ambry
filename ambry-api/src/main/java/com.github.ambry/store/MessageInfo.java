package com.github.ambry.store;

/**
 * A message info class that contains basic info about a message
 */
public class MessageInfo {
  private StoreKey key;
  private long size;
  private long timeToLiveInMs;
  private boolean isDeleted;

  public MessageInfo(StoreKey key, long size, long timeToLiveInMs) {
    this.key= key;
    this.size = size;
    this.timeToLiveInMs = timeToLiveInMs;
    this.isDeleted = false;
  }

  public MessageInfo(StoreKey key, long size, boolean deleted) {
    this.key = key;
    this.size = size;
    this.isDeleted = deleted;
    this.timeToLiveInMs = -1;
  }

  public MessageInfo(StoreKey key, long size) {
    this(key, size, -1);
  }

  public StoreKey getStoreKey() {
    return key;
  }

  public long getSize() {
    return size;
  }

  public long getTimeToLiveInMs() {
    return timeToLiveInMs;
  }

  public boolean isDeleted() {
    return isDeleted;
  }
}
