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
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Utils;
import com.github.ambry.utils.UtilsTest;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests for {@link StatsManager}.
 */
public class StatsManagerTest {
  private static final long CAPACITY = 1024 * 1024 * 1024;
  private final StatsManager statsManager;
  private final String outputFileString;
  private final File tempDir;
  private final StatsSnapshot preAggregatedSnapshot;
  private final Map<PartitionId, Store> storeMap;
  private final Random random = new Random();
  private final StatsManagerConfig config;

  /**
   * Deletes the temporary directory.
   * @throws InterruptedException
   * @throws IOException
   */
  @After
  public void cleanup() throws IOException {
    assertTrue(tempDir.getAbsolutePath() + " could not be deleted", StoreTestUtils.cleanDirectory(tempDir, true));
  }

  public StatsManagerTest() throws IOException, StoreException {
    tempDir = StoreTestUtils.createTempDirectory("nodeStatsDir-" + UtilsTest.getRandomString(10));
    outputFileString = (new File(tempDir.getAbsolutePath(), "stats_output.json")).getAbsolutePath();
    int[][] testData0 = new int[][]{{1100, 250, 600}, {330}, {500, 300}, {1000, 2000, 3000, 50, 150}};
    preAggregatedSnapshot = generateStatsSnapshot(testData0);
    int[][] testData1 = decomposeArray(testData0);
    int[][] testData2 = decomposeArray(testData0);
    storeMap = new HashMap<>();
    storeMap.put(new Partition(0, PartitionState.READ_WRITE, CAPACITY),
        new MockBlobStore(new MockBlobStoreStats(generateStatsSnapshot(testData0), false)));
    storeMap.put(new Partition(1, PartitionState.READ_WRITE, CAPACITY),
        new MockBlobStore(new MockBlobStoreStats(generateStatsSnapshot(testData1), false)));
    storeMap.put(new Partition(2, PartitionState.READ_WRITE, CAPACITY),
        new MockBlobStore(new MockBlobStoreStats(generateStatsSnapshot(testData2), false)));
    StorageManager storageManager = new MockStorageManager(storeMap);
    Properties properties = new Properties();
    properties.put("stats.output.file.path", outputFileString);
    config = new StatsManagerConfig(new VerifiableProperties(properties));
    statsManager = new StatsManager(storageManager, new ArrayList<>(storeMap.keySet()), new MetricRegistry(), config,
        new MockTime());
  }

  /**
   * Test to verify that the {@link StatsManager} is collecting, aggregating and publishing correctly using predefined
   * data sets and mocked {@link Store}s and {@link StorageManager}.
   * @throws StoreException
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testStatsManagerCollectAggregateAndPublish() throws IOException {
    StatsSnapshot actualSnapshot = new StatsSnapshot(0L, null);
    List<String> unreachableStores = Collections.EMPTY_LIST;
    for (PartitionId partitionId : storeMap.keySet()) {
      statsManager.collectAndAggregate(actualSnapshot, partitionId, unreachableStores);
    }
    assertTrue("Actual aggregated StatsSnapshot does not match with expected snapshot",
        preAggregatedSnapshot.equals(actualSnapshot));
    StatsHeader statsHeader =
        new StatsHeader(Description.QUOTA, SystemTime.getInstance().milliseconds(), storeMap.keySet().size(),
            storeMap.keySet().size(), unreachableStores);
    File outputFile = new File(outputFileString);
    if (outputFile.exists()) {
      outputFile.createNewFile();
    }
    long fileLengthBefore = outputFile.length();
    statsManager.publish(new StatsWrapper(statsHeader, actualSnapshot));
    assertTrue("Failed to publish stats to file", outputFile.length() > fileLengthBefore);
  }

  /**
   * Test to verify the behavior when dealing with a {@link Store} that is null and when a {@link StoreException} is
   * thrown by the {@link StoreStats}.
   * @throws StoreException
   * @throws IOException
   */
  @Test
  public void testStatsManagerWithProblematicStores() throws StoreException, IOException {
    Map<PartitionId, Store> problematicStoreMap = new HashMap<>();
    problematicStoreMap.put(new Partition(1, PartitionState.READ_WRITE, CAPACITY), null);
    Store exceptionStore = new MockBlobStore(new MockBlobStoreStats(null, true));
    problematicStoreMap.put(new Partition(2, PartitionState.READ_WRITE, CAPACITY), exceptionStore);
    StatsManager testStatsManager =
        new StatsManager(new MockStorageManager(problematicStoreMap), new ArrayList<>(problematicStoreMap.keySet()),
            new MetricRegistry(), config, new MockTime());
    List<String> unreachableStores = new ArrayList<>();
    StatsSnapshot actualSnapshot = new StatsSnapshot(0L, null);
    for (PartitionId partitionId : problematicStoreMap.keySet()) {
      testStatsManager.collectAndAggregate(actualSnapshot, partitionId, unreachableStores);
    }
    assertEquals("Aggregated StatsSnapshot should not contain any value", 0L, actualSnapshot.getValue().longValue());
    assertEquals("Unreachable store count mismatch with expected value", 2, unreachableStores.size());
    // test for the scenario where some stores are healthy and some are bad
    Map<PartitionId, Store> mixedStoreMap = new HashMap<>(storeMap);
    unreachableStores.clear();
    mixedStoreMap.put(new Partition(3, PartitionState.READ_WRITE, CAPACITY), null);
    mixedStoreMap.put(new Partition(4, PartitionState.READ_WRITE, CAPACITY), exceptionStore);
    testStatsManager = new StatsManager(new MockStorageManager(mixedStoreMap), new ArrayList<>(mixedStoreMap.keySet()),
        new MetricRegistry(), config, new MockTime());
    actualSnapshot = new StatsSnapshot(0L, null);
    for (PartitionId partitionId : mixedStoreMap.keySet()) {
      testStatsManager.collectAndAggregate(actualSnapshot, partitionId, unreachableStores);
    }
    assertTrue("Actual aggregated StatsSnapshot does not match with expected snapshot",
        preAggregatedSnapshot.equals(actualSnapshot));
    assertEquals("Unreachable store count mismatch with expected value", 2, unreachableStores.size());
  }

  /**
   * Test to verify {@link StatsManager} can start and shutdown properly.
   * @throws InterruptedException
   */
  @Test
  public void testStatsManagerStartAndShutdown() throws InterruptedException {
    statsManager.start();
    statsManager.shutdown();
  }

  /**
   * Test to verify {@link StatsManager} can shutdown properly before started.
   * @throws InterruptedException
   */
  @Test
  public void testShutdownBeforeStart() throws InterruptedException {
    statsManager.shutdown();
  }

  /**
   * Helper method to convert predefined quota stats represented in two dimensional array into a {@link StatsSnapshot}.
   * @param rawData the two dimensional array to be converted
   * @return the result of the conversion in a {@link StatsSnapshot}
   */
  private StatsSnapshot generateStatsSnapshot(int[][] rawData) {
    Map<String, StatsSnapshot> firstSubTreeMap = new HashMap<>();
    long totalSize = 0;
    for (int i = 0; i < rawData.length; i++) {
      Map<String, StatsSnapshot> secondSubTreeMap = new HashMap<>();
      long subTotalSize = 0;
      for (int j = 0; j < rawData[i].length; j++) {
        subTotalSize += rawData[i][j];
        secondSubTreeMap.put("innerKey_".concat(String.valueOf(j)), new StatsSnapshot(new Long(rawData[i][j]), null));
      }
      totalSize += subTotalSize;
      firstSubTreeMap.put("outerKey_".concat(String.valueOf(i)), new StatsSnapshot(subTotalSize, secondSubTreeMap));
    }
    return new StatsSnapshot(totalSize, firstSubTreeMap);
  }

  /**
   * Decompose an array randomly from a base array to generate a sub array. The original base array is obtained if the
   * modified base array is added with the returned sub array.
   * @param base the array to be decomposed (this array will be modified)
   * @return a sub array that is decomposed randomly from the given base array
   */
  private int[][] decomposeArray(int[][] base) {
    int[][] result = new int[random.nextInt(base.length) + 1][];
    for (int i = 0; i < result.length; i++) {
      result[i] = new int[random.nextInt(base[i].length + 1)];
      for (int j = 0; j < result[i].length; j++) {
        int partialValue = random.nextInt(base[i][j]);
        base[i][j] -= partialValue;
        result[i][j] = partialValue;
      }
    }
    return result;
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
    public Store getStore(PartitionId id) {
      return storeMap.get(id);
    }
  }

  /**
   * Mocked {@link BlobStore} that is intended to return a predefined {@link StoreStats} when getStoreStats is called.
   */
  private class MockBlobStore extends BlobStore {
    private final StoreStats storeStats;

    MockBlobStore(StoreStats storeStats) {
      super(UtilsTest.getRandomString(10), null, null, null, new StorageManagerMetrics(new MetricRegistry()), null,
          CAPACITY, null, null, null, null);
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
    private final boolean throwStoreException;

    MockBlobStoreStats(StatsSnapshot statsSnapshot, boolean throwStoreException) {
      super(null, null, null);
      this.statsSnapshot = statsSnapshot;
      this.throwStoreException = throwStoreException;
    }

    @Override
    public StatsSnapshot getStatsSnapshot() throws StoreException {
      if (throwStoreException) {
        throw new StoreException("Test", StoreErrorCodes.Unknown_Error);
      }
      return statsSnapshot;
    }
  }
}
