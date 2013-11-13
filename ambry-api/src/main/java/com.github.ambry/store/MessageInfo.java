package com.github.ambry.store;

/**
 * A message info class that contains basic info about a message
 */
public class MessageInfo {
  private StoreKey key;
  private long size;
  private long timeToLive;

  public MessageInfo(StoreKey key, long size, long timeToLive) {
    this.key= key;
    this.size = size;
    this.timeToLive = timeToLive;
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

  public long getTimeToLive() {
    return timeToLive;
  }
}
