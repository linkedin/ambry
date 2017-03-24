/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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

package com.github.ambry.store;

/**
 * Describes the additional segments required by a store-related entity. This is provided to the
 * {@link DiskSpaceAllocator} to initialize a disk space pool.
 */
class DiskSpaceRequirements {
  private final long segmentSizeInBytes;
  private final long segmentsNeeded;
  private final boolean swapRequired;

  /**
   * @param segmentSizeInBytes The size of each segment needed, in bytes.
   * @param segmentsNeeded The number of additional segments needed in the
   * @param swapRequired {@code true} if a swap segment of {@code segmentSizeInBytes} is required. (i.e. for compaction)
   */
  public DiskSpaceRequirements(long segmentSizeInBytes, long segmentsNeeded, boolean swapRequired) {
    if (segmentSizeInBytes < 0) {
      throw new IllegalArgumentException("Segment size cannot be negative: " + segmentSizeInBytes);
    }
    if (segmentsNeeded < 0) {
      throw new IllegalArgumentException("Segments needed cannot be negative: " + segmentsNeeded);
    }
    this.segmentSizeInBytes = segmentSizeInBytes;
    this.segmentsNeeded = segmentsNeeded;
    this.swapRequired = swapRequired;
  }

  public long getSegmentsNeeded() {
    return segmentsNeeded;
  }

  public long getSegmentSizeInBytes() {
    return segmentSizeInBytes;
  }

  public boolean isSwapRequired() {
    return swapRequired;
  }
}
