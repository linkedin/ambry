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

import com.github.ambry.config.StoreConfig;
import com.github.ambry.utils.Time;
import java.util.List;


/**
 * Responsible for managing compaction of a {@link BlobStore}. V0 implementation returns entire log segment range
 * ignoring those overlapping with {@link Journal} as part of CompactionDetails.
 */
class CompactionManager {
  private final StoreConfig storeConfig;
  private final Time time;
  private final long messageRetentionTimeInMs;

  CompactionManager(StoreConfig storeConfig, Time time) {
    this.storeConfig = storeConfig;
    this.time = time;
    this.messageRetentionTimeInMs = storeConfig.storeDeletedMessageRetentionDays * Time.SecsPerDay * Time.MsPerSec;
  }

  /**
   * Get compaction details for a given {@link BlobStore} if any
   * @param blobStore the {@link BlobStore} for which compation details are requested
   * @return the {@link CompactionDetails} containing the details about log segments that needs to be compacted.
   * {@code null} if compaction is not required
   * @throws StoreException when {@link BlobStore} is not started
   */
  CompactionDetails getCompactionDetails(BlobStore blobStore) throws StoreException {
    long usedCapacity = blobStore.getSizeInBytes();
    long totalCapacity = blobStore.getCapacityInBytes();
    CompactionDetails details = null;
    if (usedCapacity >= (storeConfig.storeMinUsedCapacityToTriggerCompactionInPercentage / 100.0) * totalCapacity) {
      List<String> potentialLogSegments = blobStore.getLogSegmentsNotInJournal();
      if (potentialLogSegments != null) {
        details = new CompactionDetails(time.milliseconds() - messageRetentionTimeInMs, potentialLogSegments);
      }
    }
    return details;
  }
}
