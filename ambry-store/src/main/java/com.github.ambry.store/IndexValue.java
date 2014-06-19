package com.github.ambry.store;

import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;

import java.nio.ByteBuffer;


/**
 * Represents the blob value stored in the index for a key.
 */

public class IndexValue {
  public enum Flags {
    Delete_Index
  }

  private static int Blob_Size_In_Bytes = 8;
  private static int Offset_Size_In_Bytes = 8;
  private static int Flag_Size_In_Bytes = 1;
  private static int Time_To_Live_Size_In_Bytes = 8;
  private static int Original_Message_Offset_Size_In_Bytes = 8;

  public static int Index_Value_Size_In_Bytes = Blob_Size_In_Bytes +
      Offset_Size_In_Bytes +
      Flag_Size_In_Bytes +
      Time_To_Live_Size_In_Bytes +
      Original_Message_Offset_Size_In_Bytes;

  private ByteBuffer value;

  public IndexValue(ByteBuffer value) {
    if (value.capacity() != Index_Value_Size_In_Bytes) {
      throw new IllegalArgumentException("Invalid buffer size");
    }
    this.value = value;
  }

  public IndexValue(long size, long offset, byte flags, long timeToLiveInMs) {
    this(size, offset, flags, timeToLiveInMs, offset);
  }

  public IndexValue(long size, long offset, byte flags, long timeToLiveInMs, long originalMessageOffset) {
    value = ByteBuffer.allocate(Index_Value_Size_In_Bytes);
    value.putLong(size);
    value.putLong(offset);
    value.put(flags);
    value.putLong(timeToLiveInMs);
    value.putLong(originalMessageOffset);
    value.position(0);
  }

  public IndexValue(long size, long offset, long timeToLiveInMs) {
    this(size, offset, (byte) 0, timeToLiveInMs);
  }

  public IndexValue(long size, long offset) {
    this(size, offset, (byte) 0, Utils.Infinite_Time);
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

  public long getOriginalMessageOffset() {
    return value.getLong(Blob_Size_In_Bytes + Offset_Size_In_Bytes + Flag_Size_In_Bytes + Time_To_Live_Size_In_Bytes);
  }

  public boolean isExpired() {
    if (getTimeToLiveInMs() != Utils.Infinite_Time && SystemTime.getInstance().milliseconds() > getTimeToLiveInMs()) {
      return true;
    }
    return false;
  }

  public void setTimeToLive(long timeToLiveInMs) {
    value.putLong(Blob_Size_In_Bytes + Offset_Size_In_Bytes + Flag_Size_In_Bytes, timeToLiveInMs);
  }

  public void setFlag(Flags flag) {
    value.put(Blob_Size_In_Bytes + Offset_Size_In_Bytes, (byte) (getFlags() | (1 << flag.ordinal())));
  }

  public void setNewOffset(long newOffset) {
    long oldOffset = getOffset();
    value.putLong(Blob_Size_In_Bytes, newOffset);
    value.putLong(Blob_Size_In_Bytes + Offset_Size_In_Bytes + Flag_Size_In_Bytes + Time_To_Live_Size_In_Bytes,
        oldOffset);
  }

  public void setNewSize(long size) {
    value.putLong(0, size);
  }

  public ByteBuffer getBytes() {
    return value;
  }
}


