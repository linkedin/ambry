package com.github.ambry.store;

import com.github.ambry.utils.Utils;


/**
 * A message info class that contains basic info about a message
 */
public class MessageInfo {
  private StoreKey key;
  private long size;
  private long expirationTimeInMs;
  private boolean isDeleted;

  public MessageInfo(StoreKey key, long size, long expirationTimeInMs) {
    this(key, size, false, expirationTimeInMs);
  }

  public MessageInfo(StoreKey key, long size, boolean deleted) {
    this(key, size, deleted, -1);
  }

  public MessageInfo(StoreKey key, long size, boolean deleted, long expirationTimeInMs) {
    this.key = key;
    this.size = size;
    this.isDeleted = deleted;
    this.expirationTimeInMs = expirationTimeInMs;
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

  public long getExpirationTimeInMs() {
    return expirationTimeInMs;
  }

  public boolean isDeleted() {
    return isDeleted;
  }

  public boolean isExpired() {
    return getExpirationTimeInMs() != Utils.Infinite_Time && System.currentTimeMillis() > getExpirationTimeInMs();
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("[MessageInfo:").append("Key-").append(key).append("Size-").append(size)
        .append("ExpirationTimeInMs-").append(expirationTimeInMs).append("IsDeleted-").append(isDeleted).append("]");
    return stringBuilder.toString();
  }
}
