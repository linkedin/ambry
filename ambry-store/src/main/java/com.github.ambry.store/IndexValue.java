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

  final static int BLOB_SIZE_IN_BYTES = 8;
  final static int OFFSET_SIZE_IN_BYTES = 8;
  final static int FLAG_SIZE_IN_BYTES = 1;
  final static int EXPIRES_AT_MS_SIZE_IN_BYTES_V0 = 8;
  final static int ORIGINAL_MESSAGE_OFFSET_SIZE_IN_BYTES = 8;

  final static int EXPIRES_AT_SECS_SIZE_IN_BYTES_V1 = 4;
  final static int OPERATION_TIME_SECS_SIZE_IN_BYTES_V1 = 4;
  final static int SERVICE_ID_SIZE_IN_BYTES = 2;
  final static int CONTAINER_ID_SIZE_IN_BYTES = 2;

  final static int INDEX_VALUE_SIZE_IN_BYTES_V0 =
      BLOB_SIZE_IN_BYTES + OFFSET_SIZE_IN_BYTES + FLAG_SIZE_IN_BYTES + EXPIRES_AT_MS_SIZE_IN_BYTES_V0
          + ORIGINAL_MESSAGE_OFFSET_SIZE_IN_BYTES;

  final static int INDEX_VALUE_SIZE_IN_BYTES_V1 =
      BLOB_SIZE_IN_BYTES + OFFSET_SIZE_IN_BYTES + FLAG_SIZE_IN_BYTES + EXPIRES_AT_SECS_SIZE_IN_BYTES_V1
          + ORIGINAL_MESSAGE_OFFSET_SIZE_IN_BYTES + OPERATION_TIME_SECS_SIZE_IN_BYTES_V1 + SERVICE_ID_SIZE_IN_BYTES +
          CONTAINER_ID_SIZE_IN_BYTES;

  private final ByteBuffer value;
  private Offset offset;
  private short version;

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

  IndexValue(long size, Offset offset, byte flags, long expiresAtMs, long operationTimeInSecs) {
    this(size, offset, flags, expiresAtMs, offset.getOffset(), operationTimeInSecs, (short) 0, (short) 0);
  }

  IndexValue(long size, Offset offset, byte flags, long expiresAtMs, long operationTimeInSecs, short serviceId,
      short containerId) {
    this(size, offset, flags, expiresAtMs, offset.getOffset(), operationTimeInSecs, serviceId, containerId);
  }

  IndexValue(long size, Offset offset, long expiresAtMs, long operationTimeInSecs) {
    this(size, offset, (byte) 0, expiresAtMs, operationTimeInSecs, (short) 0, (short) 0);
  }

  IndexValue(long size, Offset offset, long expiresAtMs, long operationTimeInSecs, short serviceId, short containerId) {
    this(size, offset, (byte) 0, expiresAtMs, operationTimeInSecs, serviceId, containerId);
  }

  IndexValue(long size, Offset offset, long operationTimeInMs) {
    this(size, offset, (byte) 0, Utils.Infinite_Time, operationTimeInMs);
  }

  IndexValue(long size, Offset offset, long operationTimeInMs, short serviceId, short containerId) {
    this(size, offset, (byte) 0, Utils.Infinite_Time, operationTimeInMs, serviceId, containerId);
  }

  private IndexValue(long size, Offset offset, byte flags, long expiresAtMs, long originalMessageOffset,
      long operationTimeInSecs, short serviceId, short containerId) {
    this.offset = offset;
    value = ByteBuffer.allocate(INDEX_VALUE_SIZE_IN_BYTES_V1);
    value.putLong(size);
    value.putLong(offset.getOffset());
    value.put(flags);
    int expiry = Math.toIntExact(expiresAtMs / Time.MsPerSec);
    value.putInt(expiresAtMs != Utils.Infinite_Time ? expiry : -1);
    value.putLong(originalMessageOffset);
    int operationTime = Math.toIntExact(operationTimeInSecs);
    value.putInt(operationTime);
    value.putShort(serviceId);
    value.putShort(containerId);
    value.position(0);
    version = 1;
  }

  long getSize() {
    return value.getLong(0);
  }

  Offset getOffset() {
    return offset;
  }

  byte getFlags() {
    return value.get(BLOB_SIZE_IN_BYTES + OFFSET_SIZE_IN_BYTES);
  }

  boolean isFlagSet(Flags flag) {
    return ((getFlags() & (1 << flag.ordinal())) != 0);
  }

  long getExpiresAtMs() {
    if (version == 0) {
      return value.getLong(BLOB_SIZE_IN_BYTES + OFFSET_SIZE_IN_BYTES + FLAG_SIZE_IN_BYTES);
    } else {
      int expiry = value.getInt(BLOB_SIZE_IN_BYTES + OFFSET_SIZE_IN_BYTES + FLAG_SIZE_IN_BYTES);
      return expiry != Utils.Infinite_Time ? (expiry * Time.MsPerSec) : Utils.Infinite_Time;
    }
  }

  long getOriginalMessageOffset() {
    if (version == 0) {
      return value.getLong(
          BLOB_SIZE_IN_BYTES + OFFSET_SIZE_IN_BYTES + FLAG_SIZE_IN_BYTES + EXPIRES_AT_MS_SIZE_IN_BYTES_V0);
    } else {
      return value.getLong(
          BLOB_SIZE_IN_BYTES + OFFSET_SIZE_IN_BYTES + FLAG_SIZE_IN_BYTES + EXPIRES_AT_SECS_SIZE_IN_BYTES_V1);
    }
  }

  int getOperationTimeInSecs() {
    if (version == 0) {
      return -1;
    } else {
      return value.getInt(
          BLOB_SIZE_IN_BYTES + OFFSET_SIZE_IN_BYTES + FLAG_SIZE_IN_BYTES + EXPIRES_AT_SECS_SIZE_IN_BYTES_V1 +
              ORIGINAL_MESSAGE_OFFSET_SIZE_IN_BYTES);
    }
  }

  short getServiceId() {
    if (version == 0) {
      return 0;
    } else {
      return value.getShort(
          BLOB_SIZE_IN_BYTES + OFFSET_SIZE_IN_BYTES + FLAG_SIZE_IN_BYTES + EXPIRES_AT_SECS_SIZE_IN_BYTES_V1 +
              ORIGINAL_MESSAGE_OFFSET_SIZE_IN_BYTES + OPERATION_TIME_SECS_SIZE_IN_BYTES_V1);
    }
  }

  short getContainerId() {
    if (version == 0) {
      return 0;
    } else {
      return value.getShort(
          BLOB_SIZE_IN_BYTES + OFFSET_SIZE_IN_BYTES + FLAG_SIZE_IN_BYTES + EXPIRES_AT_SECS_SIZE_IN_BYTES_V1 +
              ORIGINAL_MESSAGE_OFFSET_SIZE_IN_BYTES + OPERATION_TIME_SECS_SIZE_IN_BYTES_V1 + SERVICE_ID_SIZE_IN_BYTES);
    }
  }

  void setFlag(Flags flag) {
    value.put(BLOB_SIZE_IN_BYTES + OFFSET_SIZE_IN_BYTES, (byte) (getFlags() | (1 << flag.ordinal())));
  }

  void setNewOffset(Offset newOffset) {
    long originalMessageOffset = offset.getName().equals(newOffset.getName()) ? offset.getOffset() : -1;
    offset = newOffset;
    value.putLong(BLOB_SIZE_IN_BYTES, offset.getOffset());
    if (version == 0) {
      value.putLong(BLOB_SIZE_IN_BYTES + OFFSET_SIZE_IN_BYTES + FLAG_SIZE_IN_BYTES + EXPIRES_AT_MS_SIZE_IN_BYTES_V0,
          originalMessageOffset);
    } else {
      value.putLong(BLOB_SIZE_IN_BYTES + OFFSET_SIZE_IN_BYTES + FLAG_SIZE_IN_BYTES + EXPIRES_AT_SECS_SIZE_IN_BYTES_V1,
          originalMessageOffset);
    }
  }

  void clearOriginalMessageOffset() {
    if(version == 0) {
      value.putLong(BLOB_SIZE_IN_BYTES + OFFSET_SIZE_IN_BYTES + FLAG_SIZE_IN_BYTES + EXPIRES_AT_MS_SIZE_IN_BYTES_V0 , -1);
    } else{
      value.putLong(BLOB_SIZE_IN_BYTES + OFFSET_SIZE_IN_BYTES + FLAG_SIZE_IN_BYTES + EXPIRES_AT_SECS_SIZE_IN_BYTES_V1, -1);
    }
  }

  void setNewSize(long size) {
    value.putLong(0, size);
  }

  ByteBuffer getBytes() {
    return value;
  }

  @Override
  public String toString() {
    return "Offset: " + offset + ", Size: " + getSize() + ", Deleted: " + isFlagSet(Flags.Delete_Index)
        + ", ExpiresAtMs: " + getExpiresAtMs() + ", Original Message Offset: " + getOriginalMessageOffset() + (
        version == 1 ? ", OperationTimeAtSecs " + getOperationTimeInSecs() + ", ServiceId " + getServiceId() +
            ", ContainerId " + getContainerId() : "");
  }
}
