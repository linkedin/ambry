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
import java.util.Objects;


/**
 * A message info class that contains basic info about a message
 */
public class MessageInfo {

  // The life version when the operation is triggered by the requests from frontend.
  public final static short LIFE_VERSION_FROM_FRONTEND = -1;
  private final StoreKey key;
  private final long size;
  private final long expirationTimeInMs;
  //TODO Replace booleans with enum defining MessageInfoType {PUT, DELETE, TTL_UPDATE, UNDELETE}
  private final boolean isDeleted;
  private final boolean isTtlUpdated;
  private final boolean isUndeleted;
  private final Long crc;
  private final short accountId;
  private final short containerId;
  private final long operationTimeMs;
  private final short lifeVersion;

  /**
   * Construct an instance of MessageInfo.
   * @param key the {@link StoreKey} associated with this message.
   * @param size the size of this message in bytes.
   * @param expirationTimeInMs the time at which the message will expire. A value of -1 means no expiration.
   * @param accountId accountId of the blob
   * @param containerId containerId of the blob
   * @param operationTimeMs operation time in ms
   */
  public MessageInfo(StoreKey key, long size, long expirationTimeInMs, short accountId, short containerId,
      long operationTimeMs) {
    this(key, size, false, false, expirationTimeInMs, accountId, containerId, operationTimeMs);
  }

  /**
   * Construct an instance of MessageInfo.
   * @param key the {@link StoreKey} associated with this message.
   * @param size the size of this message in bytes.
   * @param accountId accountId of the blob
   * @param containerId containerId of the blob
   * @param operationTimeMs operation time in ms
   * @param lifeVersion update version of update
   */
  public MessageInfo(StoreKey key, long size, short accountId, short containerId, long operationTimeMs,
      short lifeVersion) {
    this(key, size, false, false, false, Utils.Infinite_Time, null, accountId, containerId, operationTimeMs,
        lifeVersion);
  }

  /**
   * Construct an instance of MessageInfo.
   * @param key the {@link StoreKey} associated with this message.
   * @param size the size of this message in bytes.
   * @param deleted {@code true} if the message is deleted, {@code false} otherwise
   * @param ttlUpdated {@code true} if the message's ttl has been updated, {@code false} otherwise
   * @param accountId accountId of the blob
   * @param containerId containerId of the blob
   * @param operationTimeMs operation time in ms
   */
  public MessageInfo(StoreKey key, long size, boolean deleted, boolean ttlUpdated, short accountId, short containerId,
      long operationTimeMs) {
    this(key, size, deleted, ttlUpdated, Utils.Infinite_Time, accountId, containerId, operationTimeMs);
  }

  /**
   * Construct an instance of MessageInfo.
   * @param key the {@link StoreKey} associated with this message.
   * @param size the size of this message in bytes.
   * @param deleted {@code true} if the message is deleted, {@code false} otherwise
   * @param ttlUpdated {@code true} if the message's ttl has been updated, {@code false} otherwise
   * @param undeleted {@code true} if the message is undeleted, {@code false} otherwise
   * @param accountId accountId of the blob
   * @param containerId containerId of the blob
   * @param operationTimeMs operation time in ms
   * @param lifeVersion update version of update
   */
  public MessageInfo(StoreKey key, long size, boolean deleted, boolean ttlUpdated, boolean undeleted, short accountId,
      short containerId, long operationTimeMs, short lifeVersion) {
    this(key, size, deleted, ttlUpdated, undeleted, Utils.Infinite_Time, null, accountId, containerId, operationTimeMs,
        lifeVersion);
  }

  /**
   * Construct an instance of MessageInfo.
   * @param key the {@link StoreKey} associated with this message.
   * @param size the size of this message in bytes.
   * @param deleted {@code true} if the message is deleted, {@code false} otherwise
   * @param ttlUpdated {@code true} if the message's ttl has been updated, {@code false} otherwise
   * @param expirationTimeInMs the time at which the message will expire. A value of -1 means no expiration.
   * @param accountId accountId of the blob
   * @param containerId containerId of the blob
   * @param operationTimeMs operation time in ms
   */
  public MessageInfo(StoreKey key, long size, boolean deleted, boolean ttlUpdated, long expirationTimeInMs,
      short accountId, short containerId, long operationTimeMs) {
    this(key, size, deleted, ttlUpdated, false, expirationTimeInMs, null, accountId, containerId, operationTimeMs,
        (short) 0);
  }

  /**
   * Construct an instance of MessageInfo.
   * @param key the {@link StoreKey} associated with this message.
   * @param size the size of this message in bytes.
   * @param accountId accountId of the blob
   * @param containerId containerId of the blob
   * @param operationTimeMs operation time in ms
   */
  public MessageInfo(StoreKey key, long size, short accountId, short containerId, long operationTimeMs) {
    this(key, size, Utils.Infinite_Time, accountId, containerId, operationTimeMs);
  }

  /**
   * Construct an instance of MessageInfo.
   * @param key the {@link StoreKey} associated with this message.
   * @param size the size of this message in bytes.
   * @param deleted {@code true} if the message is deleted, {@code false} otherwise
   * @param ttlUpdated {@code true} if the message's ttl has been updated, {@code false} otherwise
   * @param expirationTimeInMs the time at which the message will expire. A value of -1 means no expiration.
   * @param crc the crc associated with this message. If unavailable, pass in null.
   * @param accountId accountId of the blob
   * @param containerId containerId of the blob
   * @param operationTimeMs operation time in ms
   */
  public MessageInfo(StoreKey key, long size, boolean deleted, boolean ttlUpdated, long expirationTimeInMs, Long crc,
      short accountId, short containerId, long operationTimeMs) {
    this(key, size, deleted, ttlUpdated, false, expirationTimeInMs, crc, accountId, containerId, operationTimeMs,
        (short) 0);
  }

  /**
   * Construct an instance of MessageInfo.
   * @param key the {@link StoreKey} associated with this message.
   * @param size the size of this message in bytes.
   * @param deleted {@code true} if the message is deleted, {@code false} otherwise
   * @param ttlUpdated {@code true} if the message's ttl has been updated, {@code false} otherwise
   * @param expirationTimeInMs the time at which the message will expire. A value of -1 means no expiration.
   * @param crc the crc associated with this message. If unavailable, pass in null.
   * @param accountId accountId of the blob
   * @param containerId containerId of the blob
   * @param operationTimeMs operation time in ms
   * @param lifeVersion update version of update
   */
  public MessageInfo(StoreKey key, long size, boolean deleted, boolean ttlUpdated, boolean undeleted,
      long expirationTimeInMs, Long crc, short accountId, short containerId, long operationTimeMs, short lifeVersion) {
    if (operationTimeMs < Utils.Infinite_Time) {
      throw new IllegalArgumentException("OperationTime cannot be negative " + operationTimeMs);
    }
    this.key = key;
    this.size = size;
    this.isDeleted = deleted;
    isTtlUpdated = ttlUpdated;
    isUndeleted = undeleted;
    this.expirationTimeInMs = expirationTimeInMs;
    this.crc = crc;
    this.accountId = accountId;
    this.containerId = containerId;
    this.operationTimeMs = operationTimeMs;
    this.lifeVersion = lifeVersion;
  }

  public StoreKey getStoreKey() {
    return key;
  }

  /**
   * Get size of message in bytes
   * @return size in bytes
   */
  public long getSize() {
    return size;
  }

  public long getExpirationTimeInMs() {
    return expirationTimeInMs;
  }

  public boolean isDeleted() {
    return isDeleted;
  }

  /**
   * @return {@code true} if the message's ttl has been updated, {@code false} otherwise
   */
  public boolean isTtlUpdated() {
    return isTtlUpdated;
  }

  public boolean isUndeleted() {
    return isUndeleted;
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

  public short getLifeVersion() {
    return lifeVersion;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MessageInfo that = (MessageInfo) o;
    return size == that.size && expirationTimeInMs == that.expirationTimeInMs && isDeleted == that.isDeleted
        && isTtlUpdated == that.isTtlUpdated && isUndeleted == that.isUndeleted && accountId == that.accountId
        && containerId == that.containerId && operationTimeMs == that.operationTimeMs && Objects.equals(key, that.key)
        && lifeVersion == that.lifeVersion && Objects.equals(crc, that.crc);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, size, expirationTimeInMs, isDeleted, isTtlUpdated, isUndeleted, crc, accountId,
        containerId, operationTimeMs, lifeVersion);
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
        .append("IsTtlUpdated-")
        .append(isTtlUpdated)
        .append(",")
        .append("IsUndeleted-")
        .append(isUndeleted)
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
        .append(",")
        .append("LifeVersion-")
        .append(lifeVersion)
        .append("]");
    return stringBuilder.toString();
  }
}
