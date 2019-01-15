/*
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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

package com.github.ambry.router;

public class ByteRanges {

  /**
   * Construct a range from a start offset to an end offset.
   * @param startOffset the (inclusive) start byte offset.
   * @param endOffset the (inclusive) end byte offset.
   * @return A {@link ByteRange} with the specified offsets.
   * @throws IllegalArgumentException if the start offset is less than 0, or the end offset is less than the start.
   */
  public static ByteRange fromOffsetRange(long startOffset, long endOffset) {
    if (startOffset < 0 || endOffset < startOffset) {
      throw new IllegalArgumentException(
          "Invalid range offsets provided for ByteRange; startOffset=" + startOffset + ", endOffset=" + endOffset);
    }
    return new ClosedRange(startOffset, endOffset);
  }

  /**
   * Construct a range from a start offset to the end of an object.
   * @param startOffset The (inclusive) start byte offset.
   * @return A {@link ByteRange} with the specified start offset.
   * @throws IllegalArgumentException if the start offset is less than 0
   */
  public static ByteRange fromStartOffset(long startOffset) {
    if (startOffset < 0) {
      throw new IllegalArgumentException("Invalid range offsets provided for ByteRange; startOffset=" + startOffset);
    }
    return new OpenRange(startOffset);
  }

  /**
   * Construct a range that represents the last N bytes of an object.
   * @param lastNBytes the number of bytes to read from the end of an object.
   * @return A {@link ByteRange} representing the last N bytes of an objects.
   * @throws IllegalArgumentException if the number of bytes to read is less than or equal to 0.
   */
  public static ByteRange fromLastNBytes(long lastNBytes) {
    if (lastNBytes < 0) {
      throw new IllegalArgumentException("Invalid range offsets provided for ByteRange; lastNBytes=" + lastNBytes);
    }
    return new SuffixRange(lastNBytes);
  }

  /**
   * Only static methods in this class.
   */
  private ByteRanges() {
  }

  /**
   * A range from a start offset to an end offset.
   */
  private static class ClosedRange extends ByteRange {
    private long startOffset;
    private long endOffset;

    /**
     * @see ByteRanges#fromOffsetRange(long, long)
     */
    private ClosedRange(long startOffset, long endOffset) {
      this.startOffset = startOffset;
      this.endOffset = endOffset;
    }

    @Override
    public long getStartOffset() {
      return startOffset;
    }

    @Override
    public long getEndOffset() {
      return endOffset;
    }

    @Override
    public long getRangeSize() {
      return getEndOffset() - getStartOffset() + 1;
    }

    @Override
    public ByteRangeType getType() {
      return ByteRangeType.OFFSET_RANGE;
    }

    @Override
    public ByteRange toResolvedByteRange(long totalSize) {
      if (getStartOffset() >= totalSize) {
        throw new IllegalArgumentException("Invalid totalSize: " + totalSize + " for range: " + this);
      }
      return new ClosedRange(getStartOffset(), Math.min(getEndOffset(), totalSize - 1));
    }

    @Override
    public String toString() {
      return "ByteRange{startOffset=" + startOffset + ", endOffset=" + endOffset + '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ClosedRange that = (ClosedRange) o;
      return startOffset == that.startOffset && endOffset == that.endOffset;
    }

    @Override
    public int hashCode() {
      return 31 * Long.hashCode(startOffset) + Long.hashCode(endOffset);
    }
  }

  /**
   * A range from a start offset to the end of an object.
   */
  private static class OpenRange extends ByteRange {
    private long startOffset;

    /**
     * @see ByteRanges#fromStartOffset(long)
     */
    private OpenRange(long startOffset) {
      this.startOffset = startOffset;
    }

    @Override
    public long getStartOffset() {
      return startOffset;
    }

    @Override
    public ByteRangeType getType() {
      return ByteRangeType.FROM_START_OFFSET;
    }

    @Override
    public ByteRange toResolvedByteRange(long totalSize) {
      if (getStartOffset() >= totalSize) {
        throw new IllegalArgumentException("Invalid totalSize: " + totalSize + " for range: " + this);
      }
      return new ClosedRange(getStartOffset(), totalSize - 1);
    }

    @Override
    public String toString() {
      return "ByteRange{startOffset=" + startOffset + '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      OpenRange that = (OpenRange) o;
      return startOffset == that.startOffset;
    }

    @Override
    public int hashCode() {
      return Long.hashCode(startOffset);
    }
  }

  /**
   * A range that represents the last N bytes of an object.
   */
  private static class SuffixRange extends ByteRange {
    private long lastNBytes;

    /**
     * @see ByteRanges#fromLastNBytes(long)
     */
    private SuffixRange(long lastNBytes) {
      this.lastNBytes = lastNBytes;
    }

    @Override
    public long getLastNBytes() {
      return lastNBytes;
    }

    @Override
    public long getRangeSize() {
      return lastNBytes;
    }

    @Override
    public ByteRangeType getType() {
      return ByteRangeType.LAST_N_BYTES;
    }

    @Override
    public ByteRange toResolvedByteRange(long totalSize) {
      if (totalSize < 0) {
        throw new IllegalArgumentException("Invalid totalSize: " + totalSize + " for range: " + this);
      }
      return new ClosedRange(Math.max(totalSize - getLastNBytes(), 0), totalSize - 1);
    }

    @Override
    public String toString() {
      return "ByteRange{lastNBytes=" + lastNBytes + '}';
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SuffixRange that = (SuffixRange) o;
      return lastNBytes == that.lastNBytes;
    }

    @Override
    public int hashCode() {
      return Long.hashCode(lastNBytes);
    }
  }
}
