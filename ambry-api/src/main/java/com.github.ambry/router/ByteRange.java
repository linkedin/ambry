/*
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

package com.github.ambry.router;

/**
 * Represents a byte range for performing ranged get requests.
 */
public class ByteRange {
  private static final long UNDEFINED_OFFSET = -1;

  private final ByteRangeType type;
  private final long startOffset;
  private final long endOffset;

  /**
   * Construct a range from a start offset to an end offset.
   * @param startOffset the (inclusive) start byte offset.
   * @param endOffset the (inclusive) end byte offset.
   * @return A {@link ByteRange} with the specified offsets.
   * @throws IllegalArgumentException
   */
  public static ByteRange fromOffsetRange(long startOffset, long endOffset) {
    if (startOffset < 0 || endOffset < startOffset) {
      throw new IllegalArgumentException(
          "Invalid range offsets provided for ByteRange; startOffset=" + startOffset + ", endOffset=" + endOffset);
    }
    return new ByteRange(startOffset, endOffset, ByteRangeType.OFFSET_RANGE);
  }

  /**
   * Construct a range from a start offset to the end of an object.
   * @param startOffset The (inclusive) start byte offset.
   * @return A {@link ByteRange} with the specified start offset.
   * @throws IllegalArgumentException
   */
  public static ByteRange fromStartOffset(long startOffset) {
    if (startOffset < 0) {
      throw new IllegalArgumentException("Invalid range offsets provided for ByteRange; startOffset=" + startOffset);
    }
    return new ByteRange(startOffset, UNDEFINED_OFFSET, ByteRangeType.FROM_START_OFFSET);
  }

  /**
   * Construct a range that represents the last N bytes of an object.
   * @param lastNBytes the number of bytes to read from the end of an object.
   * @return A {@link ByteRange} representing the last N bytes of an objects.
   * @throws IllegalArgumentException
   */
  public static ByteRange fromLastNBytes(long lastNBytes) {
    if (lastNBytes < 0) {
      throw new IllegalArgumentException("Invalid range offsets provided for ByteRange; lastNBytes=" + lastNBytes);
    }
    return new ByteRange(lastNBytes, UNDEFINED_OFFSET, ByteRangeType.LAST_N_BYTES);
  }

  /**
   * Construct a range from byte offsets.
   * @param startOffset The (inclusive) start byte offset, or the number of bytes to read from the end of an object,
   *                    in the case of a {@link ByteRangeType#LAST_N_BYTES} range type.
   * @param endOffset The (inclusive) end byte offset, or {@link ByteRange#UNDEFINED_OFFSET}.
   * @param type The {@link ByteRangeType} for the range.
   */
  private ByteRange(long startOffset, long endOffset, ByteRangeType type) {
    this.type = type;
    this.startOffset = startOffset;
    this.endOffset = endOffset;
  }

  /**
   * Get the start offset for this range.
   * @return The inclusive start offset for this range.
   * @throws UnsupportedOperationException if the range does not have a defined start offset (i.e. not of the type
   *                                       {@link ByteRangeType#OFFSET_RANGE} or
   *                                       {@link ByteRangeType#FROM_START_OFFSET})
   */
  public long getStartOffset() {
    switch (getType()) {
      case FROM_START_OFFSET:
      case OFFSET_RANGE:
        return startOffset;
      default:
        throw new UnsupportedOperationException("Cannot get start offset for range type: " + type);
    }
  }

  /**
   * Get the end offset for this range.
   * @return The inclusive end offset for this range.
   * @throws UnsupportedOperationException if the range does not have a defined start offset
   *                                       (i.e. not of the type {@link ByteRangeType#OFFSET_RANGE})
   */
  public long getEndOffset() {
    switch (getType()) {
      case OFFSET_RANGE:
        return endOffset;
      default:
        throw new UnsupportedOperationException("Cannot get end offset for range type: " + type);
    }
  }

  /**
   * Get the number of bytes to read from the end of an object.
   * @return The number of bytes to read from the end of the object.
   * @throws UnsupportedOperationException if the range is not of the type {@link ByteRangeType#LAST_N_BYTES})
   */
  public long getLastNBytes() {
    switch (getType()) {
      case LAST_N_BYTES:
        return startOffset;
      default:
        throw new UnsupportedOperationException("Cannot get last N bytes for range type: " + type);
    }
  }

  /**
   * Get the {@link ByteRangeType} for the range.
   * @return the {@link ByteRangeType} for the range.
   */
  public ByteRangeType getType() {
    return type;
  }

  /**
   * @return the size of the range, in bytes.
   * @throws UnsupportedOperationException for {@link ByteRangeType#FROM_START_OFFSET} type ranges.
   */
  public long getRangeSize() {
    switch (getType()) {
      case OFFSET_RANGE:
        return getEndOffset() - getStartOffset() + 1;
      case LAST_N_BYTES:
        return getLastNBytes();
      default:
        throw new UnsupportedOperationException("Cannot determine range size for range type: " + type);
    }
  }

  /**
   * Given the total size of a blob, generate a new {@link ByteRange} of type {@link ByteRangeType#OFFSET_RANGE} with
   * defined start and end offsets that are verified to be within the supplied total blob size.
   * @param totalSize the total size of the blob that this range corresponds to.
   * @return the {@link ByteRange} with start and end offsets
   * @throws IllegalArgumentException if the byte range exceeds the total size of the blob.
   */
  public ByteRange toResolvedByteRange(long totalSize) {
    switch (getType()) {
      case LAST_N_BYTES:
        if (getLastNBytes() <= totalSize) {
          return new ByteRange(totalSize - getLastNBytes(), totalSize - 1, ByteRangeType.OFFSET_RANGE);
        }
        break;
      case FROM_START_OFFSET:
        if (getStartOffset() < totalSize) {
          return new ByteRange(getStartOffset(), totalSize - 1, ByteRangeType.OFFSET_RANGE);
        }
        break;
      case OFFSET_RANGE:
        if (getEndOffset() < totalSize) {
          return new ByteRange(getStartOffset(), getEndOffset(), ByteRangeType.OFFSET_RANGE);
        }
        break;
    }
    throw new IllegalArgumentException("ByteRange " + this + " exceeds the total blob size " + totalSize);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("ByteRange{").append("type=").append(type);
    switch (type) {
      case LAST_N_BYTES:
        sb.append(", lastNBytes=").append(getLastNBytes());
        break;
      case FROM_START_OFFSET:
        sb.append(", startOffset=").append(getStartOffset());
        break;
      case OFFSET_RANGE:
        sb.append(", startOffset=").append(getStartOffset()).append(", endOffset=").append(getEndOffset());
        break;
    }
    return sb.append('}').toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ByteRange byteRange = (ByteRange) o;

    if (startOffset != byteRange.startOffset) {
      return false;
    }
    if (endOffset != byteRange.endOffset) {
      return false;
    }
    return type == byteRange.type;
  }

  @Override
  public int hashCode() {
    int result = type != null ? type.hashCode() : 0;
    result = 31 * result + (int) (startOffset ^ (startOffset >>> 32));
    result = 31 * result + (int) (endOffset ^ (endOffset >>> 32));
    return result;
  }

  public enum ByteRangeType {
    /**
     * If this range specifies the number of bytes to read from the end of an object.
     */
    LAST_N_BYTES,

    /**
     * If this range specifies a start offset to read from to the end of an object.
     */
    FROM_START_OFFSET,

    /**
     * If this range specifies a start and end offset to read between.
     */
    OFFSET_RANGE
  }
}
