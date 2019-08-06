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
  private final long swapSegmentsInUse;
  private final String storeId;

  /**
   * @param storeId the store which the {@link DiskSpaceRequirements} is associated with.
   * @param segmentSizeInBytes the size of each segment needed, in bytes.
   * @param segmentsNeeded the number of additional segments needed in the disk space pool.
   * @param swapSegmentsInUse the number of swap segments currently in use by this entity.
   */
  DiskSpaceRequirements(String storeId, long segmentSizeInBytes, long segmentsNeeded, long swapSegmentsInUse) {
    if (segmentSizeInBytes <= 0 || segmentsNeeded < 0 || swapSegmentsInUse < 0) {
      throw new IllegalArgumentException(
          "Arguments cannot be negative. segmentSizeInBytes: " + segmentSizeInBytes + ", segmentsNeeded: "
              + segmentsNeeded + ", swapSegmentsInUse: " + swapSegmentsInUse);
    }
    this.segmentSizeInBytes = segmentSizeInBytes;
    this.segmentsNeeded = segmentsNeeded;
    this.swapSegmentsInUse = swapSegmentsInUse;
    this.storeId = storeId;
  }

  /**
   * @return the size of each segment needed, in bytes.
   */
  long getSegmentsNeeded() {
    return segmentsNeeded;
  }

  /**
   * @return the number of additional segments needed in the disk space pool
   */
  long getSegmentSizeInBytes() {
    return segmentSizeInBytes;
  }

  /**
   * @return the number of swap segments currently in use by this entity.
   */
  long getSwapSegmentsInUse() {
    return swapSegmentsInUse;
  }

  String getStoreId() {
    return storeId;
  }

  @Override
  public String toString() {
    return "DiskSpaceRequirements{segmentSizeInBytes=" + segmentSizeInBytes + ", segmentsNeeded=" + segmentsNeeded
        + ", swapSegmentsInUse=" + swapSegmentsInUse + '}';
  }
}
