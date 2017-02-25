/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.utils.Utils;


/**
 * A message info class that contains basic info about a message
 */
public class MessageInfo {
  private final StoreKey key;
  private final long size;
  private final long expirationTimeInMs;
  private final boolean isDeleted;
  private final Long crc;

  public MessageInfo(StoreKey key, long size, long expirationTimeInMs) {
    this(key, size, false, expirationTimeInMs);
  }

  public MessageInfo(StoreKey key, long size, boolean deleted) {
    this(key, size, deleted, Utils.Infinite_Time);
  }

  public MessageInfo(StoreKey key, long size, boolean deleted, long expirationTimeInMs) {
    this(key, size, deleted, expirationTimeInMs, null);
  }

  public MessageInfo(StoreKey key, long size) {
    this(key, size, Utils.Infinite_Time);
  }

  /**
   * Construct an instance of MessageInfo.
   * @param key the {@link StoreKey} associated with this message.
   * @param size the size of this message.
   * @param deleted whethe the message is deleted.
   * @param expirationTimeInMs the time at which the message will expire. A value of -1 means no expiration.
   * @param crc the crc associated with this message. If unavailable, pass in null.
   */
  public MessageInfo(StoreKey key, long size, boolean deleted, long expirationTimeInMs, Long crc) {
    this.key = key;
    this.size = size;
    this.isDeleted = deleted;
    this.expirationTimeInMs = expirationTimeInMs;
    this.crc = crc;
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

  /**
   * @return the crc associated with this message, if there is one; null otherwise.
   */
  public Long getCrc() {
    return crc;
  }

  @Override
  public String toString() {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append("[MessageInfo:")
        .append("Key-")
        .append(key)
        .append(",")
        .append("Size-")
        .append(size)
        .append(",")
        .append("ExpirationTimeInMs-")
        .append(expirationTimeInMs)
        .append(",")
        .append("IsDeleted-")
        .append(isDeleted)
        .append(",")
        .append("Crc-")
        .append(crc)
        .append("]");
    return stringBuilder.toString();
  }
}
