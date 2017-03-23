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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.Partition;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.PartitionState;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.config.StatsManagerConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import com.github.ambry.utils.UtilsTest;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.*;


public class StatsManagerTest {
  private final StatsManager statsManager;
  private final Map<PartitionId, Store> storeMap;
  private final String outputFileString;
  private final File tempDir;

  public StatsManagerTest() throws IOException, StoreException {
    tempDir = StoreTestUtils.createTempDirectory("nodeStatsDir-" + UtilsTest.getRandomString(10));
    outputFileString = (new File(tempDir.getAbsolutePath(), "stats_output.json")).getAbsolutePath();
    long[][] testData1 = new long[][]{{100, 200, 300}, {100}, {200, 300}};
    long[][] testData2 = new long[][]{{10}, {50}};
    long[][] testData3 = new long[][]{{1000}, {200}, {300}, {10, 20, 30, 40, 50}};
    List<StatsSnapshot> statsSnapshots = new ArrayList<>();
    statsSnapshots.add(generateStatsSnapshot(testData1));
    statsSnapshots.add(generateStatsSnapshot(testData2));
    statsSnapshots.add(generateStatsSnapshot(testData3));
    storeMap = new HashMap<>();
    MetricRegistry registry = new MetricRegistry();
    StorageManagerMetrics metrics = new StorageManagerMetrics(registry);
    for (int i = 0; i < 3; i++) {
      PartitionId id = new Partition(i, PartitionState.READ_WRITE, 1024 * 1024 * 1024);
      Store store =
          new MockBlobStore(UtilsTest.getRandomString(10), metrics, new MockBlobStoreStats(statsSnapshots.get(i)));
      storeMap.put(id, store);
    }
    storeMap.put(new Partition(1000, PartitionState.READ_WRITE, 1024 * 1024 * 1024), null);
    StorageManager storageManager = new MockStorageManager(storeMap);
    Properties properties = new Properties();
    properties.put("store.stats.output.file.path", outputFileString);
    StatsManagerConfig config = new StatsManagerConfig(new VerifiableProperties(properties));
    statsManager = new StatsManager(storageManager, config);
  }

  /**
   * Deletes the temporary directory.
   * @throws InterruptedException
   * @throws IOException
   */
  @After
  public void cleanup() throws IOException {
    assertTrue(tempDir.getAbsolutePath() + " could not be deleted", StoreTestUtils.cleanDirectory(tempDir, true));
  }

  /**
   * Test to verify that the {@link StatsManager} is collecting, aggregating and publishing correctly using predefined
   * data sets and mocked {@link Store}s and {@link StorageManager}.
   * @throws StoreException
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testStatsManagerCollectAggregateAndPublish() throws StoreException, IOException, InterruptedException {
    long[][] aggregatedTestData = new long[][]{{1110, 200, 300}, {350}, {500, 300}, {10, 20, 30, 40, 50}};
    StatsSnapshot aggregatedSnapshot = generateStatsSnapshot(aggregatedTestData);
    Pair<StatsSnapshot, List<String>> result = statsManager.collectAndAggregate(storeMap.keySet());
    assertTrue("Aggregated StatsSnapshot does not match with expected value",
        aggregatedSnapshot.equals(result.getFirst()));
    assertEquals("Skipped store count mismatch with expected value", 1, result.getSecond().size());
    StatsHeader statsHeader =
        new StatsHeader(Description.QUOTA, SystemTime.getInstance().milliseconds(), storeMap.keySet().size(),
            storeMap.keySet().size() - result.getSecond().size(), result.getSecond());
    File outputFile = new File(outputFileString);
    if (outputFile.exists()) {
      outputFile.createNewFile();
    }
    long fileLengthBefore = outputFile.length();
    statsManager.publish(new StatsWrapper(statsHeader, result.getFirst()));
    assertTrue("Failed to publish stats to file", outputFile.length() > fileLengthBefore);
    statsManager.shutdown();
  }

  /**
   * Test to verify {@link StatsManager} can start.
   * @throws InterruptedException
   */
  @Test
  public void testStatsManagerStart() throws InterruptedException {
    statsManager.start();
    Thread.sleep(1000);
    statsManager.shutdown();
  }

  /**
   * Helper method to convert predefined quota stats represented in two dimensional array into a {@link StatsSnapshot}.
   * @param rawData the two dimensional array to be converted
   * @return the result of the conversion in a {@link StatsSnapshot}
   */
  private StatsSnapshot generateStatsSnapshot(long[][] rawData) {
    Map<String, StatsSnapshot> firstSubTreeMap = new HashMap<>();
    long totalSize = 0;
    for (int i = 0; i < rawData.length; i++) {
      Map<String, StatsSnapshot> secondSubTreeMap = new HashMap<>();
      long subTotalSize = 0;
      for (int j = 0; j < rawData[i].length; j++) {
        subTotalSize += rawData[i][j];
        secondSubTreeMap.put("innerKey_".concat(String.valueOf(j)), new StatsSnapshot(rawData[i][j], null));
      }
      totalSize += subTotalSize;
      firstSubTreeMap.put("outerKey_".concat(String.valueOf(i)), new StatsSnapshot(subTotalSize, secondSubTreeMap));
    }
    return new StatsSnapshot(totalSize, firstSubTreeMap);
  }

  /**
   * Mocked {@link StorageManager} that is intended to have only the overwritten methods to be called and return
   * predefined values.
   */
  private class MockStorageManager extends StorageManager {
    private final Map<PartitionId, Store> storeMap;

    public MockStorageManager(Map<PartitionId, Store> map) throws StoreException {
      super(new StoreConfig(new VerifiableProperties(new Properties())), Utils.newScheduler(1, false),
          new MetricRegistry(), new ArrayList<ReplicaId>(), new MockIdFactory(), new DummyMessageStoreRecovery(),
          new DummyMessageStoreHardDelete(), SystemTime.getInstance());
      storeMap = map;
    }

    @Override
    public Set<PartitionId> getPartitionIds() {
      return storeMap.keySet();
    }

    @Override
    public Store getStore(PartitionId id) {
      return storeMap.get(id);
    }
  }

  /**
   * Mocked {@link BlobStore} that is intended to return a predefined {@link StoreStats} when getStoreStats is called.
   */
  private class MockBlobStore extends BlobStore {
    private final StoreStats storeStats;

    MockBlobStore(String storeId, StorageManagerMetrics storageManagerMetrics, StoreStats storeStats) {
      super(storeId, null, null, null, storageManagerMetrics, null, 1024, null, null, null, null);
      this.storeStats = storeStats;
    }

    @Override
    public StoreStats getStoreStats() {
      return storeStats;
    }
  }

  /**
   * Mocked {@link BlobStoreStats} to return predefined {@link StatsSnapshot} when getStatsSnapshot is called.
   */
  private class MockBlobStoreStats extends BlobStoreStats {
    private final StatsSnapshot statsSnapshot;

    MockBlobStoreStats(StatsSnapshot statsSnapshot) {
      super(null, null, null);
      this.statsSnapshot = statsSnapshot;
    }

    @Override
    public StatsSnapshot getStatsSnapshot() {
      return statsSnapshot;
    }
  }
}
