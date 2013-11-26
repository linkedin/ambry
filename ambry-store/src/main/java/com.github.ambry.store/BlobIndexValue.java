package com.github.ambry.store;

import java.nio.ByteBuffer;

/**
 * The set of data stored in the index
 */

public class BlobIndexValue {
  public enum Flags {
    Delete_Index
  }

  private static int Blob_Size_In_Bytes = 8;
  private static int Offset_Size_In_Bytes = 8;
  private static int Flag_Size_In_Bytes = 1;
  private static int Time_To_Live_Size_In_Bytes = 8;

  public static int Index_Value_Size_In_Bytes = Blob_Size_In_Bytes + Offset_Size_In_Bytes +
                                                Flag_Size_In_Bytes + Time_To_Live_Size_In_Bytes;

  private ByteBuffer value;

  public BlobIndexValue(ByteBuffer value) {
    if (value.capacity() != Index_Value_Size_In_Bytes)
      throw new IllegalArgumentException("Invalid buffer size");
    this.value = value;
  }

  public BlobIndexValue(long size, long offset, byte flags, long timeToLiveInMs) {
    value = ByteBuffer.allocate(Index_Value_Size_In_Bytes);
    value.putLong(size);
    value.putLong(offset);
    value.put(flags);
    value.putLong(timeToLiveInMs);
    value.position(0);
  }

  public BlobIndexValue(long size, long offset, long timeToLiveInMs) {
    this(size, offset, (byte)0, timeToLiveInMs);
  }

  public BlobIndexValue(long size, long offset) {
    this(size, offset, (byte)0, -1);
  }

  public long getSize() {
    return value.getLong(0);
  }

  public long getOffset() {
    return value.getLong(Blob_Size_In_Bytes);
  }

  public byte getFlags() {
    return value.get(Blob_Size_In_Bytes + Offset_Size_In_Bytes);
  }

  public boolean isFlagSet(Flags flag) {
    return ((getFlags() & (1 << flag.ordinal())) != 0);
  }

  public long getTimeToLiveInMs() {
    return value.getLong(Blob_Size_In_Bytes + Offset_Size_In_Bytes + Flag_Size_In_Bytes);
  }

  public void setTimeToLive(long timeToLiveInMs) {
    value.putLong(Blob_Size_In_Bytes + Offset_Size_In_Bytes + Flag_Size_In_Bytes, timeToLiveInMs);
  }

  public void setFlag(Flags flag) {
    value.put(Blob_Size_In_Bytes + Offset_Size_In_Bytes, (byte)(getFlags() | (1 << flag.ordinal())));
  }

  public ByteBuffer getBytes() {
    return value;
  }
}
