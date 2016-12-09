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
import java.nio.ByteBuffer;


/**
 * Represents the blob value stored in the index for a key.
 */

class IndexValue {
  enum Flags {
    Delete_Index
  }

  private static int Blob_Size_In_Bytes = 8;
  private static int Offset_Size_In_Bytes = 8;
  private static int Flag_Size_In_Bytes = 1;
  private static int Expires_At_Ms_Size_In_Bytes = 8;
  private static int Original_Message_Offset_Size_In_Bytes = 8;

  static int Index_Value_Size_In_Bytes =
      Blob_Size_In_Bytes + Offset_Size_In_Bytes + Flag_Size_In_Bytes + Expires_At_Ms_Size_In_Bytes
          + Original_Message_Offset_Size_In_Bytes;

  private final ByteBuffer value;
  private Offset offset;

  IndexValue(String logSegmentName, ByteBuffer value) {
    if (value.capacity() != Index_Value_Size_In_Bytes) {
      throw new IllegalArgumentException("Invalid buffer size");
    }
    this.value = value;
    offset = new Offset(logSegmentName, value.getLong(Blob_Size_In_Bytes));
  }

  IndexValue(long size, Offset offset, byte flags, long expiresAtMs) {
    this(size, offset, flags, expiresAtMs, offset.getOffset());
  }

  IndexValue(long size, Offset offset, long expiresAtMs) {
    this(size, offset, (byte) 0, expiresAtMs);
  }

  IndexValue(long size, Offset offset) {
    this(size, offset, (byte) 0, Utils.Infinite_Time);
  }

  private IndexValue(long size, Offset offset, byte flags, long expiresAtMs, long originalMessageOffset) {
    this.offset = offset;
    value = ByteBuffer.allocate(Index_Value_Size_In_Bytes);
    value.putLong(size);
    value.putLong(offset.getOffset());
    value.put(flags);
    value.putLong(expiresAtMs);
    value.putLong(originalMessageOffset);
    value.position(0);
  }

  long getSize() {
    return value.getLong(0);
  }

  Offset getOffset() {
    return offset;
  }

  byte getFlags() {
    return value.get(Blob_Size_In_Bytes + Offset_Size_In_Bytes);
  }

  boolean isFlagSet(Flags flag) {
    return ((getFlags() & (1 << flag.ordinal())) != 0);
  }

  long getExpiresAtMs() {
    return value.getLong(Blob_Size_In_Bytes + Offset_Size_In_Bytes + Flag_Size_In_Bytes);
  }

  long getOriginalMessageOffset() {
    return value.getLong(Blob_Size_In_Bytes + Offset_Size_In_Bytes + Flag_Size_In_Bytes + Expires_At_Ms_Size_In_Bytes);
  }

  void setFlag(Flags flag) {
    value.put(Blob_Size_In_Bytes + Offset_Size_In_Bytes, (byte) (getFlags() | (1 << flag.ordinal())));
  }

  void setNewOffset(Offset newOffset) {
    long originalMessageOffset = offset.getName().equals(newOffset.getName()) ? offset.getOffset() : -1;
    offset = newOffset;
    value.putLong(Blob_Size_In_Bytes, offset.getOffset());
    value.putLong(Blob_Size_In_Bytes + Offset_Size_In_Bytes + Flag_Size_In_Bytes + Expires_At_Ms_Size_In_Bytes,
        originalMessageOffset);
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
        + ", ExpiresAtMs: " + getExpiresAtMs() + ", Original Message Offset: " + getOriginalMessageOffset();
  }
}
