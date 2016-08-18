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
  public static final long UNDEFINED_OFFSET = -1;
  private final long startOffset;
  private final long endOffset;

  /**
   * Construct a range from the offsets parsed from the range header.
   * @param startOffset the (inclusive) start byte offset, or {@link ByteRange#UNDEFINED_OFFSET}.
   * @param endOffset the (inclusive) end byte offset, or {@link ByteRange#UNDEFINED_OFFSET}.
   * @throws InvalidByteRangeException
   */
  public ByteRange(long startOffset, long endOffset)
      throws InvalidByteRangeException {
    if (startOffset < 0 && startOffset != UNDEFINED_OFFSET || endOffset < 0 && endOffset != UNDEFINED_OFFSET
        || startOffset == UNDEFINED_OFFSET && endOffset == UNDEFINED_OFFSET
        || endOffset != UNDEFINED_OFFSET && endOffset < startOffset) {
      throw new InvalidByteRangeException(startOffset, endOffset);
    }
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
