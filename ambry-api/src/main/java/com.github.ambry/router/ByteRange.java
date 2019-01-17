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
public abstract class ByteRange {

  /**
   * Construct a range from a start offset to an end offset.
   * @deprecated Please use {@link ByteRanges#fromOffsetRange} instead.
   * @param startOffset the (inclusive) start byte offset.
   * @param endOffset the (inclusive) end byte offset.
   * @return A {@link ByteRange} with the specified offsets.
   * @throws IllegalArgumentException if the start offset is less than 0, or the end offset is less than the start.
   */
  @Deprecated
  public static ByteRange fromOffsetRange(long startOffset, long endOffset) {
    return ByteRanges.fromOffsetRange(startOffset, endOffset);
  }

  /**
   * Construct a range from a start offset to the end of an object.
   * @deprecated Please use {@link ByteRanges#fromStartOffset} instead.
   * @param startOffset The (inclusive) start byte offset.
   * @return A {@link ByteRange} with the specified start offset.
   * @throws IllegalArgumentException if the start offset is less than 0
   */
  @Deprecated
  public static ByteRange fromStartOffset(long startOffset) {
    return ByteRanges.fromStartOffset(startOffset);
  }

  /**
   * Construct a range that represents the last N bytes of an object.
   * @deprecated Please use {@link ByteRanges#fromLastNBytes} instead.
   * @param lastNBytes the number of bytes to read from the end of an object.
   * @return A {@link ByteRange} representing the last N bytes of an objects.
   * @throws IllegalArgumentException if the number of bytes to read is less than or equal to 0.
   */
  @Deprecated
  public static ByteRange fromLastNBytes(long lastNBytes) {
    return ByteRanges.fromLastNBytes(lastNBytes);
  }

  /**
   * All implementations should be defined within this package.
   */
  ByteRange() {
  }

  // Implement the following methods if they are supported for the range type.

  /**
   * @return The inclusive start offset for this range.
   * @throws UnsupportedOperationException if the range does not have a defined start offset (i.e. not of the type
   *                                       {@link ByteRangeType#OFFSET_RANGE} or
   *                                       {@link ByteRangeType#FROM_START_OFFSET})
   */
  public long getStartOffset() {
    throw new UnsupportedOperationException("Cannot get start offset for range type: " + getType());
  }

  /**
   * @return The inclusive end offset for this range.
   * @throws UnsupportedOperationException if the range does not have a defined start offset
   *                                       (i.e. not of the type {@link ByteRangeType#OFFSET_RANGE})
   */
  public long getEndOffset() {
    throw new UnsupportedOperationException("Cannot get end offset for range type: " + getType());
  }

  /**
   * @return The number of bytes to read from the end of the object.
   * @throws UnsupportedOperationException if the range is not of the type {@link ByteRangeType#LAST_N_BYTES})
   */
  public long getLastNBytes() {
    throw new UnsupportedOperationException("Cannot get last N bytes for range type: " + getType());
  }

  /**
   * @return the size of the range, in bytes.
   * @throws UnsupportedOperationException for {@link ByteRangeType#FROM_START_OFFSET} type ranges.
   */
  public long getRangeSize() {
    throw new UnsupportedOperationException("Cannot determine range size for range type: " + getType());
  }

  // implement these abstract methods for all range types.

  /**
   * @return the {@link ByteRangeType} for the range.
   */
  public abstract ByteRangeType getType();

  /**
   * Given the total size of a blob, generate a new {@link ByteRange} of type {@link ByteRangeType#OFFSET_RANGE} with
   * defined start and end offsets that are verified to be within the supplied total blob size.
   * @param totalSize the total size of the blob that this range corresponds to.
   * @return the {@link ByteRange} with start and end offsets
   * @throws IllegalArgumentException if the byte range starts past the end of the blob.
   */
  public abstract ByteRange toResolvedByteRange(long totalSize);

  @Override
  public abstract String toString();

  @Override
  public abstract boolean equals(Object o);

  @Override
  public abstract int hashCode();

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
