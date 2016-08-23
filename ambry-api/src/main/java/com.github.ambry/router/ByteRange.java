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
  private final long startOffset;
  private final long endOffset;

  /**
   * Construct a range from a start offset to the end of an object.
   * @param startOffset The (inclusive) start byte offset.
   * @return A {@link ByteRange} with the specified start offset.
   * @throws InvalidByteRangeException
   */
  public static ByteRange fromOpenRange(long startOffset)
      throws InvalidByteRangeException {
    if (startOffset < 0) {
      throw new InvalidByteRangeException(startOffset, UNDEFINED_OFFSET);
    }
    return new ByteRange(startOffset, UNDEFINED_OFFSET);
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
    return new ByteRange(UNDEFINED_OFFSET, lastNBytes);
  }

  /**
   * Construct a range from a start offset to an end offset.
   * @param startOffset the (inclusive) start byte offset.
   * @param endOffset the (inclusive) end byte offset.
   * @return A {@link ByteRange} with the specified offsets.
   * @throws InvalidByteRangeException
   */
  public static ByteRange fromClosedRange(long startOffset, long endOffset)
      throws InvalidByteRangeException {
    if (startOffset < 0 || endOffset < 0 || endOffset < startOffset) {
      throw new InvalidByteRangeException(startOffset, endOffset);
    }
    return new ByteRange(startOffset, endOffset);
  }

  /**
   * Construct a range from byte offsets.
   * @param startOffset the (inclusive) start byte offset, or {@link ByteRange#UNDEFINED_OFFSET}.
   * @param endOffset the (inclusive) end byte offset, or {@link ByteRange#UNDEFINED_OFFSET}.
   */
  private ByteRange(long startOffset, long endOffset) {
    this.startOffset = startOffset;
    this.endOffset = endOffset;
  }

  /**
   * Get the start offset for this range.
   * @return The inclusive start offset for this range, or {@link ByteRange#UNDEFINED_OFFSET}.
   */
  public long getStartOffset() {
    return startOffset;
  }

  /**
   * Get the end offset for this range.
   * @return The inclusive end offset for this range, or {@link ByteRange#UNDEFINED_OFFSET}.
   */
  public long getEndOffset() {
    return endOffset;
  }

  @Override
  public String toString() {
    return "ByteRange{startOffset=" + startOffset + ", endOffset=" + endOffset + '}';
  }
}
