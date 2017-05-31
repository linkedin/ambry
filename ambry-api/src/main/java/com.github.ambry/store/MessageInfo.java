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

  public static final short ACCOUNTID_CONTAINERID_DEFAULT_VALUE = -1;

  private final StoreKey key;
  private final long size;
  private long expirationTimeInMs = Utils.Infinite_Time;
  private boolean isDeleted = false;
  private Long crc = null;
  private short accountId;
  private short containerId;
  private long operationTimeMs;

  public static class MessageInfoBuilder {
    private final StoreKey key;
    private final long size;
    private long expirationTimeInMs = Utils.Infinite_Time;
    private boolean isDeleted = false;
    private Long crc = null;
    private short accountId = ACCOUNTID_CONTAINERID_DEFAULT_VALUE;
    private short containerId = ACCOUNTID_CONTAINERID_DEFAULT_VALUE;
    private long operationTimeMs = Utils.Infinite_Time;

    public MessageInfoBuilder(StoreKey key, long size) {
      this.key = key;
      this.size = size;
    }

    public MessageInfoBuilder setExpirationTimeMs(long expirationTimeInMs) {
      this.expirationTimeInMs = expirationTimeInMs;
      return this;
    }

    public MessageInfoBuilder setDeleted(boolean isDeleted) {
      this.isDeleted = isDeleted;
      return this;
    }

    public MessageInfoBuilder setCRC(Long crc) {
      this.crc = crc;
      return this;
    }

    public MessageInfoBuilder setAccountId(short accountId) {
      this.accountId = accountId;
      return this;
    }

    public MessageInfoBuilder setContainerId(short containerId) {
      this.containerId = containerId;
      return this;
    }

    public MessageInfoBuilder setOperationTimeMs(long operationTimeMs) {
      this.operationTimeMs = operationTimeMs;
      return this;
    }

    public MessageInfo build() {
      return new MessageInfo(key, size, isDeleted, expirationTimeInMs, crc, accountId, containerId, operationTimeMs);
    }
  }

  /**
   * Construct an instance of MessageInfo.
   * @param key the {@link StoreKey} associated with this message.
   * @param size the size of this message.
   * @param deleted whethe the message is deleted.
   * @param expirationTimeInMs the time at which the message will expire. A value of -1 means no expiration.
   * @param crc the crc associated with this message. If unavailable, pass in null.
   * @param accountId accountId of the blob
   * @param containerId containerId of the blob
   * @param operationTimeMs operation time in ms
   */
  private MessageInfo(StoreKey key, long size, boolean deleted, long expirationTimeInMs, Long crc, short accountId,
      short containerId, long operationTimeMs) {
    this.key = key;
    this.size = size;
    this.isDeleted = deleted;
    this.expirationTimeInMs = expirationTimeInMs;
    this.crc = crc;
    this.accountId = accountId;
    this.containerId = containerId;
    this.operationTimeMs = operationTimeMs;
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

  public short getAccountId() {
    return accountId;
  }

  public short getContainerId() {
    return containerId;
  }

  public long getOperationTimeMs() {
    return operationTimeMs;
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
        .append(",")
        .append("AccountId-")
        .append(accountId)
        .append(",")
        .append("ContainerId-")
        .append(containerId)
        .append(",")
        .append("OperationTimeMs-")
        .append(operationTimeMs)
        .append("]");
    return stringBuilder.toString();
  }
}
