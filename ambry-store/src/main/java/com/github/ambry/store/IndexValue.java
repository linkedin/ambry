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

import com.github.ambry.utils.Time;
import com.github.ambry.utils.Utils;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import static com.github.ambry.account.Account.*;
import static com.github.ambry.account.Container.*;


/**
 * Represents the blob value stored in the index for a key.
 *
 * Version_0
 * - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
 * | Blob Size |   Offset  |  Flags  | Expiration Time | Orig msg  |
 * | (8 bytes) | (8 bytes) | (1 byte)|     in Ms       | offset    |
 * |           |           |         |   ( 8 bytes)    | (8 bytes) |
 * - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
 *
 * Version_1 Version_2
 * - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
 * | Blob Size |   Offset  |  Flags  | Expiration Time | Orig msg  | OperationTime | ServiceId | ContainerId |
 * | (8 bytes) | (8 bytes) | (1 byte)|   in  Secs      | offset    |   in secs     | (2 bytes) | (2 bytes)   |
 * |           |           |         |   ( 4 bytes)    | (8 bytes) |   (4 bytes)   |           |             |
 * - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
 *
 * Version_3
 * - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
 * | Blob Size |   Offset  |  Flags  | Expiration Time | Orig msg  | OperationTime | ServiceId | ContainerId | LifeVersion   |
 * | (8 bytes) | (8 bytes) | (1 byte)|   in  Secs      | offset    |   in secs     | (2 bytes) | (2 bytes)   | (2 bytes)     |
 * |           |           |         |   ( 4 bytes)    | (8 bytes) |   (4 bytes)   |           |             |               |
 * - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
 *
 */
class IndexValue implements Comparable<IndexValue> {

  enum Flags {
    Delete_Index, Ttl_Update_Index, Undelete_Index
  }

  final static byte FLAGS_DEFAULT_VALUE = (byte) 0;
  final static long UNKNOWN_ORIGINAL_MESSAGE_OFFSET = -1;

  private final static int BLOB_SIZE_IN_BYTES = 8;
  private final static int OFFSET_SIZE_IN_BYTES = 8;
  private final static int FLAG_SIZE_IN_BYTES = 1;
  private final static int EXPIRES_AT_MS_SIZE_IN_BYTES_V0 = 8;
  private final static int ORIGINAL_MESSAGE_OFFSET_SIZE_IN_BYTES = 8;

  private final static int EXPIRES_AT_SECS_SIZE_IN_BYTES_V1 = 4;
  private final static int OPERATION_TIME_SECS_SIZE_IN_BYTES = 4;
  private final static int ACCOUNT_ID_SIZE_IN_BYTES = 2;
  private final static int CONTAINER_ID_SIZE_IN_BYTES = 2;
  private final static int LIFE_VERSION_SIZE_IN_BYTES = 2;

  final static int INDEX_VALUE_SIZE_IN_BYTES_V0 =
      BLOB_SIZE_IN_BYTES + OFFSET_SIZE_IN_BYTES + FLAG_SIZE_IN_BYTES + EXPIRES_AT_MS_SIZE_IN_BYTES_V0
          + ORIGINAL_MESSAGE_OFFSET_SIZE_IN_BYTES;

  final static int INDEX_VALUE_SIZE_IN_BYTES_V1_V2 =
      BLOB_SIZE_IN_BYTES + OFFSET_SIZE_IN_BYTES + FLAG_SIZE_IN_BYTES + EXPIRES_AT_SECS_SIZE_IN_BYTES_V1
          + ORIGINAL_MESSAGE_OFFSET_SIZE_IN_BYTES + OPERATION_TIME_SECS_SIZE_IN_BYTES + ACCOUNT_ID_SIZE_IN_BYTES
          + CONTAINER_ID_SIZE_IN_BYTES;

  final static int INDEX_VALUE_SIZE_IN_BYTES_V3 = INDEX_VALUE_SIZE_IN_BYTES_V1_V2 + LIFE_VERSION_SIZE_IN_BYTES;

  private long size;
  private Offset offset;
  private byte flags;
  private long expiresAtMs;
  private long originalMessageOffset;
  private final long operationTimeInMs;
  private final short accountId;
  private final short containerId;
  private final short lifeVersion;
  private final short formatVersion;

  /**
   * Constructs the {@link IndexValue} with the passed in {@link ByteBuffer} and the given {@code formatVersion}
   * @param logSegmentName the log segment name to be used to construct the offset
   * @param value the {@link ByteBuffer} representation of the {@link IndexValue}
   * @param formatVersion the format version of the {@link PersistentIndex}
   */
  IndexValue(String logSegmentName, ByteBuffer value, short formatVersion) {
    this.formatVersion = formatVersion;
    switch (formatVersion) {
      case PersistentIndex.VERSION_0:
        if (value.capacity() != INDEX_VALUE_SIZE_IN_BYTES_V0) {
          throw new IllegalArgumentException("Invalid buffer size for formatVersion 0");
        }
        size = value.getLong();
        offset = new Offset(logSegmentName, value.getLong());
        flags = value.get();
        expiresAtMs = value.getLong();
        originalMessageOffset = value.getLong();
        operationTimeInMs = (int) Utils.Infinite_Time;
        accountId = UNKNOWN_ACCOUNT_ID;
        containerId = UNKNOWN_CONTAINER_ID;
        lifeVersion = 0;
        break;
      case PersistentIndex.VERSION_1:
      case PersistentIndex.VERSION_2:
        if (value.capacity() != INDEX_VALUE_SIZE_IN_BYTES_V1_V2) {
          throw new IllegalArgumentException("Invalid buffer size for formatVersion 1/2");
        }
        size = value.getLong();
        offset = new Offset(logSegmentName, value.getLong());
        flags = value.get();
        long expiresAt = value.getInt();
        expiresAtMs = expiresAt != Utils.Infinite_Time && expiresAt >= 0 ? TimeUnit.SECONDS.toMillis(expiresAt)
            : Utils.Infinite_Time;
        originalMessageOffset = value.getLong();
        long operationTimeInSecs = value.getInt();
        operationTimeInMs = operationTimeInSecs != Utils.Infinite_Time ? TimeUnit.SECONDS.toMillis(operationTimeInSecs)
            : Utils.Infinite_Time;
        accountId = value.getShort();
        containerId = value.getShort();
        lifeVersion = 0;
        break;
      case PersistentIndex.VERSION_3:
        if (value.capacity() != INDEX_VALUE_SIZE_IN_BYTES_V3) {
          throw new IllegalArgumentException("Invalid buffer size for formatVersion 3");
        }
        size = value.getLong();
        offset = new Offset(logSegmentName, value.getLong());
        flags = value.get();
        expiresAt = value.getInt();
        expiresAtMs = expiresAt != Utils.Infinite_Time && expiresAt >= 0 ? TimeUnit.SECONDS.toMillis(expiresAt)
            : Utils.Infinite_Time;
        originalMessageOffset = value.getLong();
        operationTimeInSecs = value.getInt();
        operationTimeInMs = operationTimeInSecs != Utils.Infinite_Time ? TimeUnit.SECONDS.toMillis(operationTimeInSecs)
            : Utils.Infinite_Time;
        accountId = value.getShort();
        containerId = value.getShort();
        lifeVersion = value.getShort();
        break;
      default:
        throw new IllegalArgumentException(
            "Unsupported format version " + formatVersion + " passed in for IndexValue ");
    }
  }

  /**
   * Constructs IndexValue based on the args passed
   * @param size the size of the blob that this index value refers to
   * @param offset the {@link Offset} in the {@link Log} where the blob that this index value refers to resides
   * @param expiresAtMs the expiration time in ms at which the blob expires
   * @param operationTimeInMs operation time in ms of the entry
   * @param accountId the accountId that this blob belongs to
   * @param containerId the containerId that this blob belongs to
   */
  IndexValue(long size, Offset offset, long expiresAtMs, long operationTimeInMs, short accountId, short containerId) {
    this(size, offset, FLAGS_DEFAULT_VALUE, expiresAtMs, offset.getOffset(), operationTimeInMs, accountId, containerId,
        (short) 0);
  }

  /**
   * Constructs IndexValue based on the args passed
   * @param size the size of the blob that this index value refers to
   * @param offset the {@link Offset} in the {@link Log} where the blob that this index value refers to resides
   * @param flags the {@link Flags} that needs to be set for the Index Value
   * @param expiresAtMs the expiration time in ms at which the blob expires
   * @param operationTimeInMs operation time in ms of the entry
   * @param accountId the accountId that this blob belongs to
   * @param containerId the containerId that this blob belongs to
   * @param lifeVersion the update version of the record in the log.
   */
  IndexValue(long size, Offset offset, byte flags, long expiresAtMs, long operationTimeInMs, short accountId,
      short containerId, short lifeVersion) {
    this(size, offset, flags, expiresAtMs, offset.getOffset(), operationTimeInMs, accountId, containerId, lifeVersion);
  }

  /**
   * Constructor to copy all data from a given {@link IndexValue}.
   * @param other the given {@link IndexValue}.
   */
  IndexValue(IndexValue other) {
    this(other.getSize(), other.getOffset(), other.getFlags(), other.getExpiresAtMs(), other.getOriginalMessageOffset(),
        other.getOperationTimeInMs(), other.getAccountId(), other.getContainerId(), other.getLifeVersion());
  }

  /**
   * Constructs IndexValue based on the args passed
   * @param size the size of the blob that this index value refers to
   * @param offset the {@link Offset} in the {@link Log} where the blob that this index value refers to resides
   * @param flags the flags that needs to be set for the Index Value
   * @param expiresAtMs the expiration time in ms at which the blob expires
   * @param originalMessageOffset the offset where the PUT record pertaining to this record exists if in the same log
   *                              segment. Set to {@link #UNKNOWN_ORIGINAL_MESSAGE_OFFSET} otherwise.
   * @param operationTimeInMs the time in ms at which the operation occurred.
   * @param accountId the accountId that this blob belongs to
   * @param containerId the containerId that this blob belongs to
   * @param lifeVersion the life version of this blob.
   */
  IndexValue(long size, Offset offset, byte flags, long expiresAtMs, long originalMessageOffset,
      long operationTimeInMs, short accountId, short containerId, short lifeVersion) {
    this.size = size;
    this.offset = offset;
    this.flags = flags;
    setExpiresAtMs(expiresAtMs);
    this.originalMessageOffset = originalMessageOffset;
    this.operationTimeInMs = Utils.getTimeInMsToTheNearestSec(operationTimeInMs);
    this.accountId = accountId;
    this.containerId = containerId;
    this.lifeVersion = lifeVersion;
    formatVersion = PersistentIndex.CURRENT_VERSION;
  }

  /**
   * @return the size of the blob that this index value refers to
   */
  long getSize() {
    return size;
  }

  /**
   * @return the {@link Offset} in the {@link Log} where the blob that this index value refers to resides
   */
  Offset getOffset() {
    return offset;
  }

  /**
   * @return the {@link Flags} in the Index Value
   */
  byte getFlags() {
    return flags;
  }

  /**
   * Returns whether the passed in {@code flag} is set or not in the {@link IndexValue}
   * @param flag the {@link Flags} that needs to be checked if set in the {@link IndexValue}
   * @return {@code true} if the passed in {@link Flags} is set, {@code false} otherwise
   */
  boolean isFlagSet(Flags flag) {
    return ((getFlags() & (1 << flag.ordinal())) != 0);
  }

  /**
   * Helper function for isFlagSet(Flags.Ttl_Update_Index).
   * @return true when the Ttl_Update_Index is set.
   */
  boolean isTtlUpdate() {
    return isFlagSet(Flags.Ttl_Update_Index);
  }

  /**
   * Helper function for isFlagSet(Flags.Delete_Index).
   * @return true when the Delete_Index is set.
   */
  boolean isDelete() {
    return isFlagSet(Flags.Delete_Index);
  }

  /**
   * Helper function for isFlagSet(Flags.Undelete_Index).
   * @return true when the Undelete_Index is set.
   */
  boolean isUndelete() {
    return isFlagSet(Flags.Undelete_Index);
  }

  /**
   * Helper function to decide if this value is a put value or not.
   * @return true when it's a put record.
   */
  boolean isPut() {
    return flags == FLAGS_DEFAULT_VALUE;
  }

  /**
   * @return the expiration time of the index value in ms
   */
  long getExpiresAtMs() {
    return expiresAtMs;
  }

  /**
   * @return the offset of the PUT record related to this {@link IndexValue} if it is in the same log segment.
   * {@link #UNKNOWN_ORIGINAL_MESSAGE_OFFSET} otherwise.
   */
  long getOriginalMessageOffset() {
    return originalMessageOffset;
  }

  /**
   * @return the operation time in ms of the index value
   */
  long getOperationTimeInMs() {
    return operationTimeInMs;
  }

  /**
   * @return the accountId of the {@link IndexValue}
   */
  short getAccountId() {
    return accountId;
  }

  /**
   * @return the containerId of the {@link IndexValue}
   */
  short getContainerId() {
    return containerId;
  }

  /**
   * True when the life version is not from frontend requests.
   * @param lifeVersion the given life version.
   * @return true when it's not from frontend requests.
   */
  static boolean hasLifeVersion(short lifeVersion) {
    return lifeVersion > MessageInfo.LIFE_VERSION_FROM_FRONTEND;
  }

  /**
   * @return the lifeVersion of the {@link IndexValue}
   */
  short getLifeVersion() {
    return lifeVersion;
  }

  /**
   * Sets the {@link Flags} for the {@link IndexValue}
   * @param flag the {@link Flags} that needs to be set in the {@link IndexValue}
   */
  void setFlag(Flags flag) {
    flags = (byte) (flags | (1 << flag.ordinal()));
  }

  /**
   * Clears the provided {@code flag}
   * @param flag the flag to clear
   */
  void clearFlag(Flags flag) {
    flags = (byte) (flags & ~(1 << flag.ordinal()));
  }

  /**
   * Updates the {@link Offset} of the {@link IndexValue}
   * @param newOffset the new {@link Offset} to be updated for the {@link IndexValue}
   */
  void setNewOffset(Offset newOffset) {
    Offset oldOffset = offset;
    offset = newOffset;
    setOriginalMessageOffset(oldOffset);
  }

  /**
   * Sets the offset of the PUT record if the offset is in the same {@link LogSegment}
   * @param originalMessageOffset the offset to set as the original message offset
   */
  void setOriginalMessageOffset(Offset originalMessageOffset) {
    this.originalMessageOffset =
        offset.getName().equals(originalMessageOffset.getName()) ? originalMessageOffset.getOffset()
            : UNKNOWN_ORIGINAL_MESSAGE_OFFSET;
  }

  void clearOriginalMessageOffset() {
    originalMessageOffset = UNKNOWN_ORIGINAL_MESSAGE_OFFSET;
  }

  /**
   * Sets the size of the {@link IndexValue}
   * @param size the size that needs to be set for the {@link IndexValue}
   */
  void setNewSize(long size) {
    this.size = size;
  }

  /**
   * @return the {@link ByteBuffer} representation of this {@link IndexValue}
   */
  ByteBuffer getBytes() {
    ByteBuffer value;
    switch (formatVersion) {
      case PersistentIndex.VERSION_0:
        value = ByteBuffer.allocate(INDEX_VALUE_SIZE_IN_BYTES_V0);
        value.putLong(size);
        value.putLong(offset.getOffset());
        value.put(flags);
        value.putLong(expiresAtMs);
        value.putLong(originalMessageOffset);
        value.position(0);
        break;
      case PersistentIndex.VERSION_1:
      case PersistentIndex.VERSION_2:
        value = ByteBuffer.allocate(INDEX_VALUE_SIZE_IN_BYTES_V1_V2);
        value.putLong(size);
        value.putLong(offset.getOffset());
        value.put(flags);
        value.putInt(expiresAtMs != Utils.Infinite_Time ? (int) (expiresAtMs / Time.MsPerSec) : (int) expiresAtMs);
        value.putLong(originalMessageOffset);
        value.putInt(operationTimeInMs != Utils.Infinite_Time ? (int) (operationTimeInMs / Time.MsPerSec)
            : (int) operationTimeInMs);
        value.putShort(accountId);
        value.putShort(containerId);
        value.position(0);
        break;
      case PersistentIndex.VERSION_3:
        value = ByteBuffer.allocate(INDEX_VALUE_SIZE_IN_BYTES_V3);
        value.putLong(size);
        value.putLong(offset.getOffset());
        value.put(flags);
        value.putInt(expiresAtMs != Utils.Infinite_Time ? (int) (expiresAtMs / Time.MsPerSec) : (int) expiresAtMs);
        value.putLong(originalMessageOffset);
        value.putInt(operationTimeInMs != Utils.Infinite_Time ? (int) (operationTimeInMs / Time.MsPerSec)
            : (int) operationTimeInMs);
        value.putShort(accountId);
        value.putShort(containerId);
        value.putShort(lifeVersion);
        value.position(0);
        break;
      default:
        throw new IllegalArgumentException("Unsupported formatVersion " + formatVersion + " for IndexValue ");
    }
    return value;
  }

  @Override
  public String toString() {
    return "Offset: " + offset + ", Size: " + getSize() + ", Deleted: " + isDelete() + ", TTL Updated: " + isTtlUpdate()
        + ", Undelete: " + isUndelete() + ", ExpiresAtMs: " + getExpiresAtMs() + ", Original Message Offset: "
        + getOriginalMessageOffset() + (formatVersion != PersistentIndex.VERSION_0 ? (", OperationTimeInMs "
        + getOperationTimeInMs() + ", AccountId " + getAccountId() + ", ContainerId " + getContainerId()) : "") + (
        formatVersion > PersistentIndex.VERSION_2 ? ", Life Version:" + lifeVersion : "");
  }

  /**
   * @return the format version of this {@link IndexValue}
   */
  short getFormatVersion() {
    return formatVersion;
  }

  void setExpiresAtMs(long expiresAtMsLocal) {
    // if expiry in secs > Integer.MAX_VALUE, treat it as permanent blob
    if (TimeUnit.MILLISECONDS.toSeconds(expiresAtMsLocal) > Integer.MAX_VALUE) {
      expiresAtMs = Utils.Infinite_Time;
    } else if (expiresAtMsLocal != Utils.Infinite_Time && expiresAtMsLocal < 0) {
      expiresAtMs = Utils.Infinite_Time;
    } else {
      expiresAtMs = Utils.getTimeInMsToTheNearestSec(expiresAtMsLocal);
    }
  }

  /**
   * {@inheritDoc}
   * <p/>
   * Compares by {@link Offset}.
   * @param o the {@link IndexValue} to compare to.
   * @return a negative integer, zero, or a positive integer as the offset of this {@link IndexValue} is less than,
   * equal to, or greater than the offset of {@code o}.
   */
  @Override
  public int compareTo(IndexValue o) {
    return offset.compareTo(o.offset);
  }
}
