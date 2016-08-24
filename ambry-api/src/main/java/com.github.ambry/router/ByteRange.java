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
  static final long UNDEFINED_OFFSET = -1;

  private final ByteRangeType type;
  private final long startOffset;
  private final long endOffset;

  /**
   * Construct a range from a start offset to an end offset.
   * @param startOffset the (inclusive) start byte offset.
   * @param endOffset the (inclusive) end byte offset.
   * @return A {@link ByteRange} with the specified offsets.
   * @throws InvalidByteRangeException
   */
  public static ByteRange fromOffsetRange(long startOffset, long endOffset)
      throws InvalidByteRangeException {
    if (startOffset < 0 || endOffset < 0 || endOffset < startOffset) {
      throw new InvalidByteRangeException(startOffset, endOffset);
    }
    return new ByteRange(startOffset, endOffset, ByteRangeType.OFFSET_RANGE);
  }

  /**
   * Construct a range from a start offset to the end of an object.
   * @param startOffset The (inclusive) start byte offset.
   * @return A {@link ByteRange} with the specified start offset.
   * @throws InvalidByteRangeException
   */
  public static ByteRange fromStartOffset(long startOffset)
      throws InvalidByteRangeException {
    if (startOffset < 0) {
      throw new InvalidByteRangeException(startOffset, UNDEFINED_OFFSET);
    }
    return new ByteRange(startOffset, UNDEFINED_OFFSET, ByteRangeType.FROM_START_OFFSET);
  }

  /**
   * Construct a range that represents the last N bytes of an object.
   * @param lastNBytes the number of bytes to read from the end of an object.
   * @return A {@link ByteRange} representing the last N bytes of an objects.
   * @throws InvalidByteRangeException
   */
  public static ByteRange fromLastNBytes(long lastNBytes)
      throws InvalidByteRangeException {
    if (lastNBytes < 0) {
      throw new InvalidByteRangeException(UNDEFINED_OFFSET, lastNBytes);
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
   * @throws IllegalStateException if the range does not have a defined start offset (i.e. not of the type
   *                               {@link ByteRangeType#OFFSET_RANGE} or {@link ByteRangeType#FROM_START_OFFSET})
   */
  public long getStartOffset() {
    switch (type) {
      case FROM_START_OFFSET:
      case OFFSET_RANGE:
        return startOffset;
      default:
        throw new IllegalStateException("Cannot get start offset for range type: " + type);
    }
  }

  /**
   * Get the end offset for this range.
   * @return The inclusive end offset for this range.
   * @throws IllegalStateException if the range does not have a defined start offset
   *                               (i.e. not of the type {@link ByteRangeType#OFFSET_RANGE})
   */
  public long getEndOffset() {
    switch (type) {
      case OFFSET_RANGE:
        return endOffset;
      default:
        throw new IllegalStateException("Cannot get end offset for range type: " + type);
    }
  }

  /**
   * Get the number of bytes to read from the end of an object.
   * @return The number of bytes to read from the end of the object.
   * @throws IllegalStateException if the range is not of the type {@link ByteRangeType#LAST_N_BYTES})
   */
  public long getLastNBytes() {
    switch (type) {
      case LAST_N_BYTES:
        return startOffset;
      default:
        throw new IllegalStateException("Cannot get last N bytes for range type: " + type);
    }
  }

  /**
   * Get the {@link ByteRangeType} for the range.
   * @return the {@link ByteRangeType} for the range.
   */
  public ByteRangeType getType() {
    return type;
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
