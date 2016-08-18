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

import static com.github.ambry.router.ByteRange.UNDEFINED_OFFSET;


/**
 * a byte range with defined start and end offsets that are verified to be within the supplied total blob size.
 */
class ValidatedByteRange {
  private final long startOffset;
  private final long endOffset;

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("ValidatedByteRange{");
    sb.append("startOffset=").append(startOffset);
    sb.append(", endOffset=").append(endOffset);
    sb.append('}');
    return sb.toString();
  }

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
    long rawStartOffset = range.getStartOffset();
    long rawEndOffset = range.getEndOffset();
    startOffset = (rawStartOffset == UNDEFINED_OFFSET) ? totalSize - rawEndOffset : rawStartOffset;
    endOffset = (rawStartOffset == UNDEFINED_OFFSET || rawEndOffset == UNDEFINED_OFFSET) ? totalSize - 1 : rawEndOffset;
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

  /**
   * Test if a {@link ByteRange} is within the total size of a blob.
   * @param range The raw {@link ByteRange} to test.
   * @param totalSize The total size of the blob.
   * @return {@code true} if this range is within the total size of the blob.
   */
  private boolean validateRange(ByteRange range, long totalSize) {
    long rawStartOffset = range.getStartOffset();
    long rawEndOffset = range.getEndOffset();
    if (rawEndOffset == UNDEFINED_OFFSET) {
      return rawStartOffset < totalSize;
    } else if (rawStartOffset == UNDEFINED_OFFSET){
      return rawEndOffset <= totalSize;
    } else {
      return rawEndOffset < totalSize;
    }
  }
}
