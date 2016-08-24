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
 * a byte range with defined start and end offsets that are verified to be within the supplied total blob size.
 */
class ValidatedByteRange {
  private final long startOffset;
  private final long endOffset;

  /**
   * Construct a byte range with defined start and end offsets that are verified to be within
   * the supplied total blob size.
   * @param range a raw {@link ByteRange}
   * @param totalSize The total size of the blob.
   * @throws IllegalArgumentException
   */
  ValidatedByteRange(ByteRange range, long totalSize)
      throws InvalidByteRangeException {
    if (!validateRange(range, totalSize)) {
      throw new InvalidByteRangeException(range, totalSize);
    }
    switch (range.getType()) {
      case LAST_N_BYTES:
        startOffset = totalSize - range.getLastNBytes();
        endOffset = totalSize - 1;
        break;
      case FROM_START_OFFSET:
        startOffset = range.getStartOffset();
        endOffset = totalSize - 1;
        break;
      default:
        startOffset = range.getStartOffset();
        endOffset = range.getEndOffset();
        break;
    }
  }

  /**
   * Get the defined (inclusive) start offset for this range, with respect to the total object size.
   * @return The start offset.
   */
  public long getStartOffset() {
    return startOffset;
  }

  /**
   * Get the defined (inclusive) end offset for this range, with respect to the total object size.
   * @return The end offset.
   */
  public long getEndOffset() {
    return endOffset;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("ValidatedByteRange{");
    sb.append("startOffset=").append(startOffset);
    sb.append(", endOffset=").append(endOffset);
    sb.append('}');
    return sb.toString();
  }

  /**
   * Test if a {@link ByteRange} is within the total size of a blob.
   * @param range The raw {@link ByteRange} to test.
   * @param totalSize The total size of the blob.
   * @return {@code true} if this range is within the total size of the blob.
   */
  private boolean validateRange(ByteRange range, long totalSize) {
    switch (range.getType()) {
      case LAST_N_BYTES:
        return range.getLastNBytes() <= totalSize;
      case FROM_START_OFFSET:
        return range.getStartOffset() < totalSize;
      default:
        return range.getEndOffset() < totalSize;
    }
  }
}
