/**
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

import java.util.List;


/**
 * CompactionPolicy is used to determine the log segments that needs to be compacted for a given {@link BlobStore}
 */
interface CompactionPolicy {

  /**
   * Get compaction details for a given {@link BlobStore} containing information about the log segments to compact.
   * {@code null} if there isn't any to compact. {@link BlobStore} is expected to call into this with the required
   * arguments
   * @param totalCapacity Total capacity of the {@link BlobStore}
   * @param usedCapacity Used capacity of the {@link BlobStore}
   * @param segmentCapacity Segment capacity of a {@link LogSegment}
   * @param segmentHeaderSize Segment header size of a {@link LogSegment}
   * @param logSegmentsNotInJournal {@link List<String> } of log segment names which has non overlapping entries with
   *                                {@link Journal}
   * @param blobStoreStats {@link BlobStoreStats} pertaining to the {@link BlobStore} for which
   * {@link CompactionDetails} are requested
   * @return {@link CompactionDetails} containing the details of segments to be compacted
   * @throws StoreException
   */
  CompactionDetails getCompactionDetails(long totalCapacity, long usedCapacity, long segmentCapacity,
      long segmentHeaderSize, List<String> logSegmentsNotInJournal, BlobStoreStats blobStoreStats)
      throws StoreException;
}
