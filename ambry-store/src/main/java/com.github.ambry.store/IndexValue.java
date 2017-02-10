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


/**
 * Represents the blob value stored in the index for a key.
 *
 * Version_0
 * - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
 * | Blob Size |   Offset  |  Flags  | Expiration Time | Orig msg  |
 * | (8 bytes) | (8 bytes) | (1 byte)|     in Ms       | offset    |
 * |           |           |         |     ( 8 bytes)  | (8 bytes) |
 * - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
 *
 * Version_1
 * - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
 * | Blob Size |   Offset  |  Flags  | Expiration Time | Orig msg  | OperationTime | ServiceId | ContainerId |
 * | (8 bytes) | (8 bytes) | (1 byte)|   in  Secs      | offset    |  (4 bytes)    | (2 bytes) | (2 bytes)   |
 * |           |           |         |     ( 4 bytes)  | (8 bytes) |               |           |             |
 * - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
 *
 */
class IndexValue {
  enum Flags {
    Delete_Index
  }

  private final static int BLOB_SIZE_IN_BYTES = 8;
  private final static int OFFSET_SIZE_IN_BYTES = 8;
  private final static int FLAG_SIZE_IN_BYTES = 1;
  private final static int EXPIRES_AT_MS_SIZE_IN_BYTES_V0 = 8;
  private final static int ORIGINAL_MESSAGE_OFFSET_SIZE_IN_BYTES = 8;

  private final static int EXPIRES_AT_SECS_SIZE_IN_BYTES_V1 = 4;
  private final static int OPERATION_TIME_SECS_SIZE_IN_BYTES_V1 = 4;
  private final static int SERVICE_ID_SIZE_IN_BYTES = 2;
  private final static int CONTAINER_ID_SIZE_IN_BYTES = 2;

  // relative offsets for version 0
  private final static int OFFSET_RELATIVE_OFFSET = BLOB_SIZE_IN_BYTES;
  private final static int FLAGS_RELATIVE_OFFSET = OFFSET_RELATIVE_OFFSET + OFFSET_SIZE_IN_BYTES;
  private final static int EXPIRES_AT_RELATIVE_OFFSET = FLAGS_RELATIVE_OFFSET + FLAG_SIZE_IN_BYTES;
  private final static int ORIGINAL_MESSAGE_OFFSET_V0_RELATIVE_OFFSET =
      EXPIRES_AT_RELATIVE_OFFSET + EXPIRES_AT_MS_SIZE_IN_BYTES_V0;

  // relative offsets for version 1
  private final static int ORIGINAL_MESSAGE_OFFSET_V1_RELATIVE_OFFSET =
      EXPIRES_AT_RELATIVE_OFFSET + EXPIRES_AT_SECS_SIZE_IN_BYTES_V1;
  private final static int OPERATION_TIME_RELATIVE_OFFSET =
      ORIGINAL_MESSAGE_OFFSET_V1_RELATIVE_OFFSET + ORIGINAL_MESSAGE_OFFSET_SIZE_IN_BYTES;
  private final static int SERVICE_ID_RELATIVE_OFFSET =
      OPERATION_TIME_RELATIVE_OFFSET + OPERATION_TIME_SECS_SIZE_IN_BYTES_V1;
  private final static int CONTAINER_ID_RELATIVE_OFFSET = SERVICE_ID_RELATIVE_OFFSET + SERVICE_ID_SIZE_IN_BYTES;

  final static int INDEX_VALUE_SIZE_IN_BYTES_V0 =
      ORIGINAL_MESSAGE_OFFSET_V0_RELATIVE_OFFSET + ORIGINAL_MESSAGE_OFFSET_SIZE_IN_BYTES;

  final static int INDEX_VALUE_SIZE_IN_BYTES_V1 = CONTAINER_ID_RELATIVE_OFFSET + CONTAINER_ID_SIZE_IN_BYTES;

  private final ByteBuffer value;
  private Offset offset;
  private short version;

  /**
   * Constructs the {@link IndexValue} with the passed in {@link ByteBuffer} and the given {@code version}
   * @param logSegmentName the log segment name to be used to construct the offset
   * @param value the {@link ByteBuffer} representation of the {@link IndexValue}
   * @param version the version of the index value
   */
  IndexValue(String logSegmentName, ByteBuffer value, short version) {
    if (version == 0 && value.capacity() != INDEX_VALUE_SIZE_IN_BYTES_V0) {
      throw new IllegalArgumentException("Invalid buffer size for version 0");
    }
    if (version == 1 && value.capacity() != INDEX_VALUE_SIZE_IN_BYTES_V1) {
      throw new IllegalArgumentException("Invalid buffer size for version 1");
    }
    this.version = version;
    this.value = value;
    offset = new Offset(logSegmentName, value.getLong(BLOB_SIZE_IN_BYTES));
  }

  /**
   * Constructs IndexValue based on the args passed
   * @param size the size of the blob that this index value refers to
   * @param offset the {@link Offset} in the {@link Log} where the blob that this index value refers to resides
   * @param flags the {@link Flags} that needs to be set for the Index Value
   * @param expiresAtMs the expiration time in ms at which the blob expires
   * @param operationTimeInSecs operation time of the entry in secs
   * @param serviceId the serviceId that this blob belongs to
   * @param containerId the containerId that this blob belongs to
   */
  IndexValue(long size, Offset offset, byte flags, long expiresAtMs, long operationTimeInSecs, short serviceId,
      short containerId) {
    this(size, offset, flags, expiresAtMs, offset.getOffset(), operationTimeInSecs, serviceId, containerId);
  }

  /**
   * Constructs IndexValue based on the args passed
   * @param size the size of the blob that this index value refers to
   * @param offset the {@link Offset} in the {@link Log} where the blob that this index value refers to resides
   * @param flags the flags that needs to be set for the Index Value
   * @param expiresAtMs the expiration time in ms at which the blob expires
   * @param originalMessageOffset the original message offset where the Put record pertaining to a delete record exists
   *                              in the same log segment. Set to -1 otherwise.
   * @param operationTimeInSecs operation time of the entry in secs
   * @param serviceId the serviceId that this blob belongs to
   * @param containerId the containerId that this blob belongs to
   */
  private IndexValue(long size, Offset offset, byte flags, long expiresAtMs, long originalMessageOffset,
      long operationTimeInSecs, short serviceId, short containerId) {
    this.offset = offset;
    value = ByteBuffer.allocate(INDEX_VALUE_SIZE_IN_BYTES_V1);
    value.putLong(size);
    value.putLong(offset.getOffset());
    value.put(flags);
    int expiryInSecs = Math.toIntExact(expiresAtMs / Time.MsPerSec);
    value.putInt(expiresAtMs != Utils.Infinite_Time ? expiryInSecs : -1);
    value.putLong(originalMessageOffset);
    int operationTime = Math.toIntExact(operationTimeInSecs);
    value.putInt(operationTime);
    value.putShort(serviceId);
    value.putShort(containerId);
    value.position(0);
    version = PersistentIndex.VERSION_1;
  }

  /**
   * @return the size of the blob that this index value refers to
   */
  long getSize() {
    return value.getLong(0);
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
    return value.get(FLAGS_RELATIVE_OFFSET);
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
   * @return the expiration time of the index value in ms
   */
  long getExpiresAtMs() {
    if (version == 0) {
      return value.getLong(EXPIRES_AT_RELATIVE_OFFSET);
    } else {
      int expiresAt = value.getInt(EXPIRES_AT_RELATIVE_OFFSET);
      return expiresAt != Utils.Infinite_Time ? (expiresAt * Time.MsPerSec) : Utils.Infinite_Time;
    }
  }

  /**
   * @return the original message offset of the {@link IndexValue}
   */
  long getOriginalMessageOffset() {
    if (version == 0) {
      return value.getLong(ORIGINAL_MESSAGE_OFFSET_V0_RELATIVE_OFFSET);
    } else {
      return value.getLong(ORIGINAL_MESSAGE_OFFSET_V1_RELATIVE_OFFSET);
    }
  }

  /**
   * @return the operation time of the index value
   */
  int getOperationTimeInSecs() {
    if (version == 0) {
      return -1;
    } else {
      return value.getInt(OPERATION_TIME_RELATIVE_OFFSET);
    }
  }

  /**
   * @return the serviceId of the {@link IndexValue}
   */
  short getServiceId() {
    if (version == 0) {
      return 0;
    } else {
      return value.getShort(SERVICE_ID_RELATIVE_OFFSET);
    }
  }

  /**
   * @return the containerId of the {@link IndexValue}
   */
  short getContainerId() {
    if (version == 0) {
      return 0;
    } else {
      return value.getShort(CONTAINER_ID_RELATIVE_OFFSET);
    }
  }

  /**
   * Sets the {@link Flags} for the {@link IndexValue}
   * @param flag the {@link Flags} that needs to be set in the {@link IndexValue}
   */
  void setFlag(Flags flag) {
    value.put(FLAGS_RELATIVE_OFFSET, (byte) (getFlags() | (1 << flag.ordinal())));
  }

  /**
   * Updates the {@link Offset} of the {@link IndexValue}
   * @param newOffset the new {@link Offset} to be updated for the {@link IndexValue}
   */
  void setNewOffset(Offset newOffset) {
    long originalMessageOffset = offset.getName().equals(newOffset.getName()) ? offset.getOffset() : -1;
    offset = newOffset;
    value.putLong(OFFSET_RELATIVE_OFFSET, offset.getOffset());
    if (version == 0) {
      value.putLong(ORIGINAL_MESSAGE_OFFSET_V0_RELATIVE_OFFSET, originalMessageOffset);
    } else {
      value.putLong(ORIGINAL_MESSAGE_OFFSET_V1_RELATIVE_OFFSET, originalMessageOffset);
    }
  }

  void clearOriginalMessageOffset() {
    if(version == 0) {
      value.putLong(BLOB_SIZE_IN_BYTES + OFFSET_SIZE_IN_BYTES + FLAG_SIZE_IN_BYTES + EXPIRES_AT_MS_SIZE_IN_BYTES_V0 , -1);
    } else{
      value.putLong(BLOB_SIZE_IN_BYTES + OFFSET_SIZE_IN_BYTES + FLAG_SIZE_IN_BYTES + EXPIRES_AT_SECS_SIZE_IN_BYTES_V1, -1);
    }
  }

  /**
   * Sets the size of the {@link IndexValue}
   * @param size the size that needs to be set for the {@link IndexValue}
   */
  void setNewSize(long size) {
    value.putLong(0, size);
  }

  /**
   * @return the {@link ByteBuffer} representation of this {@link IndexValue}
   */
  ByteBuffer getBytes() {
    return value;
  }

  @Override
  public String toString() {
    return "Offset: " + offset + ", Size: " + getSize() + ", Deleted: " + isFlagSet(Flags.Delete_Index)
        + ", ExpiresAtMs: " + getExpiresAtMs() + ", Original Message Offset: " + getOriginalMessageOffset() + (
        version == PersistentIndex.VERSION_1 ? ", OperationTimeAtSecs " + getOperationTimeInSecs() + ", ServiceId "
            + getServiceId() + ", ContainerId " + getContainerId() : "");
  }
}

/**
 * Builder class to assist in contructing the {@link IndexValue}
 */
class IndexValueBuilder {
  private long size;
  private Offset offset;
  private byte flags = (byte) 0;
  private long expiresAtMs = Utils.Infinite_Time;
  private long operationTimeInSecs = Utils.Infinite_Time;
  private short serviceId = (short) 0;
  private short containerId = (short) 0;

  /**
   * Initializes the {@link IndexValueBuilder} with size and {@link Offset}
   * @param size the size of the blob that the {@link IndexValue} will refer to
   * @param offset the {@link Offset} in the {@link Log} where the blob that this index value refers to resides
   */
  IndexValueBuilder(long size, Offset offset) {
    this.size = size;
    this.offset = offset;
  }

  /**
   * Sets the flags for the Index Value
   * @param flags that needs to be set
   * @return the {@link IndexValueBuilder} for this instance
   */
  IndexValueBuilder flags(byte flags) {
    this.flags = flags;
    return this;
  }

  /**
   * Sets the expiration time for the Index Value
   * @param expiresAtMs the expiration time in ms at which the blob expires
   * @return the {@link IndexValueBuilder} for this instance
   */
  IndexValueBuilder expirationTimeAtMs(long expiresAtMs) {
    this.expiresAtMs = expiresAtMs;
    return this;
  }

  /**
   * Sets the operation time for the Index Value
   * @param operationTimeInSecs operation time of the entry in secs
   * @return the {@link IndexValueBuilder} for this instance
   */
  IndexValueBuilder operationTimeInSecs(long operationTimeInSecs) {
    this.operationTimeInSecs = operationTimeInSecs;
    return this;
  }

  /**
   * Sets the serviceId for the Index Value
   * @param serviceId the serviceId that this blob belongs to
   * @return the {@link IndexValueBuilder} for this instance
   */
  IndexValueBuilder serviceId(short serviceId) {
    this.serviceId = serviceId;
    return this;
  }

  /**
   * Sets the containerId for the Index Value
   * @param containerId the containerId that this blob belongs to
   * @return the {@link IndexValueBuilder} for this instance
   */
  IndexValueBuilder containerId(short containerId) {
    this.containerId = containerId;
    return this;
  }

  /**
   * Builds the {@link IndexValue} based on the argiments set using the {@link IndexValueBuilder}
   * @return the {@link IndexValue} thus constructed
   */
  public IndexValue build() {
    return new IndexValue(size, offset, flags, expiresAtMs, operationTimeInSecs, serviceId, containerId);
  }
}
