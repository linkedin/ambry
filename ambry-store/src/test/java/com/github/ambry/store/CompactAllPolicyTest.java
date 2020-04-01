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
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Time;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.Test;


public class CompactAllPolicyTest {

  // the properties that will used to generate a StoreConfig. Clear before use if required.
  private final Properties properties = new Properties();
  private final Time time = new MockTime();
  private StoreConfig config;
  private MockBlobStore blobStore;
  private long messageRetentionTimeInMs;
  private CompactionPolicy compactionPolicy = null;

  /**
   * Instantiates {@link CompactionPolicyTest} with the required cast
   * @throws InterruptedException
   */
  public CompactAllPolicyTest() throws InterruptedException {
    Pair<MockBlobStore, StoreConfig> initState = CompactionPolicyTest.initializeBlobStore(properties, time, -1, -1, -1);
    config = initState.getSecond();
    messageRetentionTimeInMs = TimeUnit.DAYS.toMillis(config.storeDeletedMessageRetentionDays);
    blobStore = initState.getFirst();
    compactionPolicy = new CompactAllPolicy(config, time);
  }

  /**
   * Tests {@link CompactionManager#getCompactionDetails(BlobStore)}
   * @throws StoreException
   * @throws InterruptedException
   */
  @Test
  public void testGetCompactionDetailsTest() throws StoreException, InterruptedException {
    blobStore.logSegmentsNotInJournal = CompactionPolicyTest.generateRandomLogSegmentName(1);
    CompactionPolicyTest.verifyCompactionDetails(
        new CompactionDetails(time.milliseconds() - messageRetentionTimeInMs, blobStore.logSegmentsNotInJournal),
        blobStore, compactionPolicy);

    // random no of valid logSegments
    for (int i = 0; i < 3; i++) {
      int logSegmentCount = TestUtils.RANDOM.nextInt(10) + 1;
      blobStore.logSegmentsNotInJournal = CompactionPolicyTest.generateRandomLogSegmentName(logSegmentCount);
      CompactionPolicyTest.verifyCompactionDetails(
          new CompactionDetails(time.milliseconds() - messageRetentionTimeInMs, blobStore.logSegmentsNotInJournal),
          blobStore, compactionPolicy);
    }
  }
}
