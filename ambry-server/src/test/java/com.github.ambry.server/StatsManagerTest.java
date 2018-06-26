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

package com.github.ambry.server;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.MockClusterMap;
import com.github.ambry.clustermap.MockDataNodeId;
import com.github.ambry.clustermap.MockPartitionId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.config.DiskManagerConfig;
import com.github.ambry.config.StatsManagerConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.store.FindInfo;
import com.github.ambry.store.FindToken;
import com.github.ambry.store.MessageWriteSet;
import com.github.ambry.store.StorageManager;
import com.github.ambry.store.Store;
import com.github.ambry.store.StoreErrorCodes;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreGetOptions;
import com.github.ambry.store.StoreInfo;
import com.github.ambry.store.StoreKey;
import com.github.ambry.store.StoreStats;
import com.github.ambry.store.TimeRange;
import com.github.ambry.utils.MockTime;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.UtilsTest;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests for {@link StatsManager}.
 */
public class StatsManagerTest {
  private static final int MAX_ACCOUNT_COUNT = 10;
  private static final int MIN_ACCOUNT_COUNT = 5;
  private static final int MAX_CONTAINER_COUNT = 6;
  private static final int MIN_CONTAINER_COUNT = 3;
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
    File[] files = tempDir.listFiles();
    if (files == null) {
      throw new IOException("Could not list files in directory: " + tempDir.getAbsolutePath());
    }
    for (File file : files) {
      assertTrue(file + " could not be deleted", file.delete());
    }
  }

  public StatsManagerTest() throws IOException, StoreException {
    tempDir = Files.createTempDirectory("nodeStatsDir-" + UtilsTest.getRandomString(10)).toFile();
    tempDir.deleteOnExit();
    outputFileString = (new File(tempDir.getAbsolutePath(), "stats_output.json")).getAbsolutePath();
    storeMap = new HashMap<>();
    preAggregatedSnapshot = generateRandomSnapshot();
    Pair<StatsSnapshot, StatsSnapshot> baseSliceAndNewSlice = new Pair<>(preAggregatedSnapshot, null);
    List<ReplicaId> replicaIds = new ArrayList<>();
    PartitionId partitionId;
    DataNodeId dataNodeId;
    for (int i = 0; i < 2; i++) {
      dataNodeId = new MockDataNodeId(Collections.singletonList(new Port(6667, PortType.PLAINTEXT)),
          Collections.singletonList("/tmp"), "DC1");
      partitionId =
          new MockPartitionId(i, MockClusterMap.DEFAULT_PARTITION_CLASS, Arrays.asList((MockDataNodeId) dataNodeId), 0);
      baseSliceAndNewSlice = decomposeSnapshot(baseSliceAndNewSlice.getFirst());
      storeMap.put(partitionId, new MockStore(new MockStoreStats(baseSliceAndNewSlice.getSecond(), false)));
      replicaIds.add(partitionId.getReplicaIds().get(0));
    }
    storeMap.put(new MockPartitionId(2, MockClusterMap.DEFAULT_PARTITION_CLASS),
        new MockStore(new MockStoreStats(baseSliceAndNewSlice.getFirst(), false)));
    StorageManager storageManager = new MockStorageManager(storeMap);
    Properties properties = new Properties();
    properties.put("stats.output.file.path", outputFileString);
    config = new StatsManagerConfig(new VerifiableProperties(properties));
    statsManager = new StatsManager(storageManager, replicaIds, new MetricRegistry(), config, new MockTime());
  }

  /**
   * Test to verify that the {@link StatsManager} is collecting, aggregating and publishing correctly using randomly
   * generated data sets and mock {@link Store}s and {@link StorageManager}.
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
        new StatsHeader(StatsHeader.StatsDescription.QUOTA, SystemTime.getInstance().milliseconds(),
            storeMap.keySet().size(), storeMap.keySet().size(), unreachableStores);
    File outputFile = new File(outputFileString);
    if (outputFile.exists()) {
      outputFile.createNewFile();
    }
    long fileLengthBefore = outputFile.length();
    statsManager.publish(new StatsWrapper(statsHeader, actualSnapshot));
    assertTrue("Failed to publish stats to file", outputFile.length() > fileLengthBefore);
  }

  /**
   * Test to verify the behavior when dealing with {@link Store} that is null and when {@link StoreException} is thrown.
   * @throws StoreException
   * @throws IOException
   */
  @Test
  public void testStatsManagerWithProblematicStores() throws StoreException, IOException {
    DataNodeId dataNodeId = new MockDataNodeId(Collections.singletonList(new Port(6667, PortType.PLAINTEXT)),
        Collections.singletonList("/tmp"), "DC1");
    Map<PartitionId, Store> problematicStoreMap = new HashMap<>();
    PartitionId partitionId1 =
        new MockPartitionId(1, MockClusterMap.DEFAULT_PARTITION_CLASS, Arrays.asList((MockDataNodeId) dataNodeId), 0);
    PartitionId partitionId2 =
        new MockPartitionId(2, MockClusterMap.DEFAULT_PARTITION_CLASS, Arrays.asList((MockDataNodeId) dataNodeId), 0);
    problematicStoreMap.put(partitionId1, null);
    Store exceptionStore = new MockStore(new MockStoreStats(null, true));
    problematicStoreMap.put(partitionId2, exceptionStore);
    StatsManager testStatsManager = new StatsManager(new MockStorageManager(problematicStoreMap),
        Arrays.asList(partitionId1.getReplicaIds().get(0), partitionId2.getReplicaIds().get(0)), new MetricRegistry(),
        config, new MockTime());
    List<String> unreachableStores = new ArrayList<>();
    StatsSnapshot actualSnapshot = new StatsSnapshot(0L, null);
    for (PartitionId partitionId : problematicStoreMap.keySet()) {
      testStatsManager.collectAndAggregate(actualSnapshot, partitionId, unreachableStores);
    }
    assertEquals("Aggregated StatsSnapshot should not contain any value", 0L, actualSnapshot.getValue());
    assertEquals("Unreachable store count mismatch with expected value", 2, unreachableStores.size());
    // test for the scenario where some stores are healthy and some are bad
    Map<PartitionId, Store> mixedStoreMap = new HashMap<>(storeMap);
    unreachableStores.clear();
    PartitionId partitionId3 =
        new MockPartitionId(3, MockClusterMap.DEFAULT_PARTITION_CLASS, Arrays.asList((MockDataNodeId) dataNodeId), 0);
    PartitionId partitionId4 =
        new MockPartitionId(4, MockClusterMap.DEFAULT_PARTITION_CLASS, Arrays.asList((MockDataNodeId) dataNodeId), 0);
    mixedStoreMap.put(partitionId3, null);
    mixedStoreMap.put(partitionId4, exceptionStore);
    testStatsManager = new StatsManager(new MockStorageManager(mixedStoreMap),
        Arrays.asList(partitionId3.getReplicaIds().get(0), partitionId4.getReplicaIds().get(0)), new MetricRegistry(),
        config, new MockTime());
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
   * Test to verify {@link StatsManager} can shutdown properly even before it's started.
   * @throws InterruptedException
   */
  @Test
  public void testShutdownBeforeStart() throws InterruptedException {
    statsManager.shutdown();
  }

  /**
   * Generate a random, two levels of nesting (accountId, containerId) {@link StatsSnapshot} for testing aggregation
   * @return a {@link StatsSnapshot} with random structure and values
   */
  private StatsSnapshot generateRandomSnapshot() {
    Map<String, StatsSnapshot> accountMap = new HashMap<>();
    long totalSize = 0;
    for (int i = 0; i < random.nextInt(MAX_ACCOUNT_COUNT - MIN_ACCOUNT_COUNT + 1) + MIN_ACCOUNT_COUNT; i++) {
      Map<String, StatsSnapshot> containerMap = new HashMap<>();
      long subTotalSize = 0;
      for (int j = 0; j < random.nextInt(MAX_CONTAINER_COUNT - MIN_CONTAINER_COUNT + 1) + MIN_CONTAINER_COUNT; j++) {
        long validSize = random.nextInt(2501) + 500;
        subTotalSize += validSize;
        containerMap.put("containerId_".concat(String.valueOf(j)), new StatsSnapshot(validSize, null));
      }
      totalSize += subTotalSize;
      accountMap.put("accountId_".concat(String.valueOf(i)), new StatsSnapshot(subTotalSize, containerMap));
    }
    return new StatsSnapshot(totalSize, accountMap);
  }

  /**
   * Decompose a nested (accountId, containerId) {@link StatsSnapshot} randomly from a given base snapshot into two
   * slices of the original base snapshot. The given base snapshot is unmodified.
   * @param baseSnapshot the base snapshot to be used for the decomposition
   * @return A {@link Pair} of {@link StatsSnapshot}s whose first element is what remains from the base snapshot
   * after the decomposition and whose second element is the random slice taken from the original base snapshot.
   */
  private Pair<StatsSnapshot, StatsSnapshot> decomposeSnapshot(StatsSnapshot baseSnapshot) {
    int accountSliceCount = random.nextInt(baseSnapshot.getSubMap().size() + 1);
    Map<String, StatsSnapshot> accountMap1 = new HashMap<>();
    Map<String, StatsSnapshot> accountMap2 = new HashMap<>();
    long partialTotalSize = 0;
    for (Map.Entry<String, StatsSnapshot> accountEntry : baseSnapshot.getSubMap().entrySet()) {
      if (accountSliceCount > 0) {
        int containerSliceCount = random.nextInt(accountEntry.getValue().getSubMap().size() + 1);
        Map<String, StatsSnapshot> containerMap1 = new HashMap<>();
        Map<String, StatsSnapshot> containerMap2 = new HashMap<>();
        long partialSubTotalSize = 0;
        for (Map.Entry<String, StatsSnapshot> containerEntry : accountEntry.getValue().getSubMap().entrySet()) {
          if (containerSliceCount > 0) {
            long baseValue = containerEntry.getValue().getValue();
            long partialValue = random.nextInt((int) baseValue);
            containerMap1.put(containerEntry.getKey(), new StatsSnapshot(baseValue - partialValue, null));
            containerMap2.put(containerEntry.getKey(), new StatsSnapshot(partialValue, null));
            partialSubTotalSize += partialValue;
            containerSliceCount--;
          } else {
            containerMap1.put(containerEntry.getKey(), containerEntry.getValue());
          }
        }
        accountMap1.put(accountEntry.getKey(),
            new StatsSnapshot(accountEntry.getValue().getValue() - partialSubTotalSize, containerMap1));
        accountMap2.put(accountEntry.getKey(), new StatsSnapshot(partialSubTotalSize, containerMap2));
        partialTotalSize += partialSubTotalSize;
        accountSliceCount--;
      } else {
        accountMap1.put(accountEntry.getKey(), accountEntry.getValue());
      }
    }
    return new Pair<>(new StatsSnapshot(baseSnapshot.getValue() - partialTotalSize, accountMap1),
        new StatsSnapshot(partialTotalSize, accountMap2));
  }

  /**
   * Mocked {@link StorageManager} that is intended to have only the overwritten methods to be called and return
   * predefined values.
   */
  private static class MockStorageManager extends StorageManager {
    private static final VerifiableProperties VPROPS = new VerifiableProperties(new Properties());
    private final Map<PartitionId, Store> storeMap;

    MockStorageManager(Map<PartitionId, Store> map) throws StoreException {
      super(new StoreConfig(VPROPS), new DiskManagerConfig(VPROPS), null, new MetricRegistry(), new ArrayList<>(), null,
          null, null, null, SystemTime.getInstance());
      storeMap = map;
    }

    @Override
    public Store getStore(PartitionId id) {
      return storeMap.get(id);
    }
  }

  /**
   * Mocked {@link Store} that is intended to return a predefined {@link StoreStats} when getStoreStats is called.
   */
  private class MockStore implements Store {
    private final StoreStats storeStats;

    MockStore(StoreStats storeStats) {
      this.storeStats = storeStats;
    }

    @Override
    public void start() throws StoreException {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public StoreInfo get(List<? extends StoreKey> ids, EnumSet<StoreGetOptions> storeGetOptions) throws StoreException {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void put(MessageWriteSet messageSetToWrite) throws StoreException {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void delete(MessageWriteSet messageSetToDelete) throws StoreException {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void updateTtl(MessageWriteSet messageSetToUpdate) throws StoreException {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public FindInfo findEntriesSince(FindToken token, long maxTotalSizeOfEntries) throws StoreException {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public Set<StoreKey> findMissingKeys(List<StoreKey> keys) throws StoreException {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public StoreStats getStoreStats() {
      return storeStats;
    }

    @Override
    public boolean isKeyDeleted(StoreKey key) throws StoreException {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public long getSizeInBytes() {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public boolean isEmpty() {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public void shutdown() throws StoreException {
      throw new IllegalStateException("Not implemented");
    }
  }

  /**
   * Mocked {@link StoreStats} to return predefined {@link StatsSnapshot} when getStatsSnapshot is called.
   */
  private class MockStoreStats implements StoreStats {
    private final StatsSnapshot statsSnapshot;
    private final boolean throwStoreException;

    MockStoreStats(StatsSnapshot statsSnapshot, boolean throwStoreException) {
      this.statsSnapshot = statsSnapshot;
      this.throwStoreException = throwStoreException;
    }

    @Override
    public Pair<Long, Long> getValidSize(TimeRange timeRange) throws StoreException {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public StatsSnapshot getStatsSnapshot(long referenceTimeInMs) throws StoreException {
      if (throwStoreException) {
        throw new StoreException("Test", StoreErrorCodes.Unknown_Error);
      }
      return statsSnapshot;
    }
  }
}
