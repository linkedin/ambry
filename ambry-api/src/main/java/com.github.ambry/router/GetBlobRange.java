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
public class GetBlobRange {
  public static final long UNDEFINED_OFFSET = -1;
  private final long startOffset;
  private final long endOffset;

  /**
   * Construct a range from the offsets parsed from the range header.
   * @param startOffset the (inclusive) start byte offset, or {@link GetBlobRange#UNDEFINED_OFFSET}.
   * @param endOffset the (inclusive) end byte offset, or {@link GetBlobRange#UNDEFINED_OFFSET}.
   */
  public GetBlobRange(long startOffset, long endOffset) {
    if (startOffset < UNDEFINED_OFFSET || endOffset < UNDEFINED_OFFSET
        || startOffset == UNDEFINED_OFFSET && endOffset == UNDEFINED_OFFSET
        || startOffset == UNDEFINED_OFFSET && endOffset == 0
        || endOffset != UNDEFINED_OFFSET && endOffset < startOffset) {
      throw new IllegalArgumentException(
          String.format("Invalid range offsets provided; startOffset: %d, endOffset: %d", startOffset, endOffset));
    }
    this.startOffset = startOffset;
    this.endOffset = endOffset;
  }

  /**
   * Test if this range is within the total size of a blob.
   * @param totalSize The total size of the blob.
   * @return {@code true} if this range is within the total size of the blob.
   */
  public boolean withinTotalSize(long totalSize) {
    if (startOffset != UNDEFINED_OFFSET && endOffset != UNDEFINED_OFFSET) {
      // Range in the form "a-b", where "a" is the start byte and "b" is the end byte.
      return endOffset < totalSize;
    } else if (endOffset == UNDEFINED_OFFSET) {
      // Range in the form "a-", where "a" is the start byte.
      return startOffset < totalSize;
    } else {
      // Range in the form "-b", where "b" denotes the last "b" bytes in an object.
      return endOffset <= totalSize;
    }
  }

  /**
   * Get the start byte offset for a range, given the total size of a blob. The user must ensure that the range is
   * within the total blob size by calling {@link GetBlobRange#withinTotalSize(long)} before calling this method.
   * @param totalSize The total size of the blob.
   * @return A defined (inclusive) start offset with respect to the total size of the blob
   */
  public long getStartOffset(long totalSize) {
    // If the range is in the form "-b", the start offset should be the start of the last "b" bytes of the object.
    if (startOffset == UNDEFINED_OFFSET) {
      return totalSize - endOffset;
    }
    return startOffset;
  }

  /**
   * Get the end byte offset for a range, given the total size of an blob. The user must ensure that the range is
   * within the total blob size by calling {@link GetBlobRange#withinTotalSize(long)} before calling this method.
   * @param totalSize The total size of the blob.
   * @return A defined (inclusive) end offset with respect to the total size of the blob
   */
  public long getEndOffset(long totalSize) {
    // If the range is in the form "-b", that is, requesting the last "b" bytes of the object, the end offset should be
    // the last byte in the file. If the range is in the form "a-", that is, requesting all bytes including and after
    // "a", the end offset should be the last byte in the file.
    if (endOffset == UNDEFINED_OFFSET || startOffset == UNDEFINED_OFFSET) {
      return totalSize - 1;
    }
    return endOffset;
  }

  @Override
  public String toString() {
    return "GetBlobRange{startOffset=" + startOffset + ", endOffset=" + endOffset + '}';
  }
}
