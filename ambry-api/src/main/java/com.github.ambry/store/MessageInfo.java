package com.github.ambry.store;

/**
 * A message info class that contains basic info about a message
 */
public class MessageInfo {
  private StoreKey key;
  private long size;
  private long ttl;

  public MessageInfo(StoreKey key, long size, long ttl) {
    this.key= key;
    this.size = size;
    this.ttl = ttl;
  }

  public StoreKey getKey() {
    return key;
  }

  public long getSize() {
    return size;
  }

  public long getTTL() {
    return ttl;
  }
}
