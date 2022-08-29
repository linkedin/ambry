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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.github.ambry.utils.Utils;
import java.util.Objects;


/**
 * A message info class that contains basic info about a message
 */
@JsonDeserialize(builder=MessageInfo.Builder.class)
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
    this.isTtlUpdated = ttlUpdated;
    this.isUndeleted = undeleted;
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

  /**
   * True when the life version is not from frontend requests.
   * @return true when it's not from frontend requests.
   */
  public static boolean hasLifeVersion(short lifeVersion) {
    return lifeVersion > MessageInfo.LIFE_VERSION_FROM_FRONTEND;
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

  /**
   * A builder class for {@link MessageInfo}.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonPOJOBuilder(withPrefix = "")
  public static class Builder {
    private StoreKey key;
    private short accountId;
    private short containerId;
    private long operationTimeMs;
    private long size;

    private long expirationTimeInMs = Utils.Infinite_Time;
    private boolean isDeleted = false;
    private boolean isTtlUpdated = false;
    private boolean isUndeleted = false;
    private Long crc = null;
    private short lifeVersion = 0;

    /**
     * Empty constructor for jackson library
     */
    public Builder() {
    }

    /**
     * Constructor to create a builder.
     * @param key The {@link StoreKey} associated with {@link MessageInfo}.
     * @param size The size of this message in bytes.
     * @param accountId accountId of the blob.
     * @param containerId containerId of the blob.
     * @param operationTimeMs operation time in ms.
     */
    public Builder(StoreKey key, long size, short accountId, short containerId, long operationTimeMs) {
      this.key = key;
      this.size = size;
      this.accountId = accountId;
      this.containerId = containerId;
      this.operationTimeMs = operationTimeMs;
    }

    /**
     * Constructor to create a builder from {@link MessageInfo}.
     * @param info The {@link MessageInfo} to build from.
     */
    public Builder(final MessageInfo info) {
      this.key = info.getStoreKey();
      this.accountId = info.getAccountId();
      this.containerId = info.getContainerId();
      this.operationTimeMs = info.getOperationTimeMs();
      this.size = info.getSize();
      this.expirationTimeInMs = info.getExpirationTimeInMs();
      this.isDeleted = info.isDeleted();
      this.isTtlUpdated = info.isTtlUpdated();
      this.isUndeleted = info.isUndeleted();
      this.crc = info.getCrc();
      this.lifeVersion = info.getLifeVersion();
    }

    /**
     * Builds a {@link MessageInfo} object.
     * @return A {@link MessageInfo} object.
     */
    public MessageInfo build() {
      return new MessageInfo(key, size, isDeleted, isTtlUpdated, isUndeleted, expirationTimeInMs, crc, accountId,
          containerId, operationTimeMs, lifeVersion);
    }

    /**
     * Sets the key of the {@link MessageInfo} to build.
     * @param key the key to set.
     * @return This builder.
     */
    public Builder storeKey(StoreKey key) {
      this.key = key;
      return this;
    }

    /**
     * Sets the accountId of the {@link MessageInfo} to build.
     * @param accountId the accountId to set.
     * @return This builder.
     */
    public Builder accountId(short accountId) {
      this.accountId = accountId;
      return this;
    }

    /**
     * Sets the containerId of the {@link MessageInfo} to build.
     * @param containerId the containerId to set.
     * @return This builder.
     */
    public Builder containerId(short containerId) {
      this.containerId = containerId;
      return this;
    }

    /**
     * Sets the operationTime in ms of the {@link MessageInfo} to build.
     * @param operationTimeMs the operationTime to set.
     * @return This builder.
     */
    public Builder operationTimeMs(long operationTimeMs) {
      this.operationTimeMs = operationTimeMs;
      return this;
    }

    /**
     * Sets the size of the {@link MessageInfo} to build.
     * @param size the size to set.
     * @return This builder.
     */
    public Builder size(long size) {
      this.size = size;
      return this;
    }

    /**
     * Sets expirationTime in ms of the {@link MessageInfo} to build.
     * @param expirationTimeInMs the expirationTime to set
     * @return This builder.
     */
    public Builder expirationTimeInMs(long expirationTimeInMs) {
      this.expirationTimeInMs = expirationTimeInMs;
      return this;
    }

    /**
     * Sets isDeleted flag of the {@link MessageInfo} to build.
     * @param isDeleted the isDeleted to set.
     * @return This builder.
     */
    @JsonProperty("deleted")
    public Builder isDeleted(boolean isDeleted) {
      this.isDeleted = isDeleted;
      return this;
    }

    /**
     * Sets isTtlUpdated flag of the {@link MessageInfo} to build.
     * @param isTtlUpdated the isTtlUpdated to set.
     * @return This builder.
     */
    @JsonProperty("ttlUpdated")
    public Builder isTtlUpdated(boolean isTtlUpdated) {
      this.isTtlUpdated = isTtlUpdated;
      return this;
    }

    /**
     * Sets isUndeleted flag of the {@link MessageInfo} to build.
     * @param isUndeleted the isUndeleted to set.
     * @return This builder.
     */
    @JsonProperty("undeleted")
    public Builder isUndeleted(boolean isUndeleted) {
      this.isUndeleted = isUndeleted;
      return this;
    }

    /**
     * Sets crc of the {@link MessageInfo} to build.
     * @param crc the crc to set.
     * @return This builder.
     */
    public Builder crc(Long crc) {
      this.crc = crc;
      return this;
    }

    /**
     * Sets the lifeVersion of the {@link MessageInfo} to build.
     * @param lifeVersion the lifeVersion to set.
     * @return This builder.
     */
    public Builder lifeVersion(short lifeVersion) {
      this.lifeVersion = lifeVersion;
      return this;
    }
  }
}
