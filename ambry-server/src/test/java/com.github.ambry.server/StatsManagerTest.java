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
import com.github.ambry.config.StatsManagerConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.Port;
import com.github.ambry.network.PortType;
import com.github.ambry.replication.FindToken;
import com.github.ambry.store.FindInfo;
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
import com.github.ambry.utils.Utils;
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
import java.util.concurrent.CountDownLatch;
import org.codehaus.jackson.map.ObjectMapper;
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
  private final Map<PartitionId, StatsSnapshot> partitionToSnapshot;
  private final List<ReplicaId> replicas;
  private final Random random = new Random();
  private final ObjectMapper mapper = new ObjectMapper();
  private final StatsManagerConfig config;

  /**
   * Deletes the temporary directory.
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
    partitionToSnapshot = new HashMap<>();
    preAggregatedSnapshot = generateRandomSnapshot().get(StatsReportType.ACCOUNT_REPORT);
    Pair<StatsSnapshot, StatsSnapshot> baseSliceAndNewSlice = new Pair<>(preAggregatedSnapshot, null);
    replicas = new ArrayList<>();
    PartitionId partitionId;
    DataNodeId dataNodeId;
    for (int i = 0; i < 2; i++) {
      dataNodeId = new MockDataNodeId(Collections.singletonList(new Port(6667, PortType.PLAINTEXT)),
          Collections.singletonList("/tmp"), "DC1");
      partitionId = new MockPartitionId(i, MockClusterMap.DEFAULT_PARTITION_CLASS,
          Collections.singletonList((MockDataNodeId) dataNodeId), 0);
      baseSliceAndNewSlice = decomposeSnapshot(baseSliceAndNewSlice.getFirst());
      Map<StatsReportType, StatsSnapshot> snapshotsByType = new HashMap<>();
      snapshotsByType.put(StatsReportType.ACCOUNT_REPORT, baseSliceAndNewSlice.getSecond());
      StoreStats storeStats = new MockStoreStats(snapshotsByType, false);
      storeMap.put(partitionId, new MockStore(storeStats));
      partitionToSnapshot.put(partitionId, snapshotsByType.get(StatsReportType.ACCOUNT_REPORT));
      replicas.add(partitionId.getReplicaIds().get(0));
    }
    Map<StatsReportType, StatsSnapshot> snapshotsByType = new HashMap<>();
    snapshotsByType.put(StatsReportType.ACCOUNT_REPORT, baseSliceAndNewSlice.getFirst());
    partitionId = new MockPartitionId(2, MockClusterMap.DEFAULT_PARTITION_CLASS);
    storeMap.put(partitionId, new MockStore(new MockStoreStats(snapshotsByType, false)));
    partitionToSnapshot.put(partitionId, snapshotsByType.get(StatsReportType.ACCOUNT_REPORT));
    StorageManager storageManager = new MockStorageManager(storeMap);
    Properties properties = new Properties();
    properties.put("stats.output.file.path", outputFileString);
    config = new StatsManagerConfig(new VerifiableProperties(properties));
    statsManager = new StatsManager(storageManager, replicas, new MetricRegistry(), config, new MockTime());
  }

  /**
   * Test to verify that the {@link StatsManager} is collecting, aggregating and publishing correctly using randomly
   * generated data sets and mock {@link Store}s and {@link StorageManager}.
   * @throws IOException
   */
  @Test
  public void testStatsManagerCollectAggregateAndPublish() throws IOException {
    StatsSnapshot actualSnapshot = new StatsSnapshot(0L, null);
    List<PartitionId> unreachablePartitions = Collections.emptyList();
    for (PartitionId partitionId : storeMap.keySet()) {
      statsManager.collectAndAggregate(actualSnapshot, partitionId, unreachablePartitions);
    }
    assertTrue("Actual aggregated StatsSnapshot does not match with expected snapshot",
        preAggregatedSnapshot.equals(actualSnapshot));
    List<String> unreachableStores = statsManager.examineUnreachablePartitions(unreachablePartitions);
    StatsHeader statsHeader =
        new StatsHeader(StatsHeader.StatsDescription.STORED_DATA_SIZE, SystemTime.getInstance().milliseconds(),
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
   */
  @Test
  public void testStatsManagerWithProblematicStores() throws IOException, StoreException {
    DataNodeId dataNodeId = new MockDataNodeId(Collections.singletonList(new Port(6667, PortType.PLAINTEXT)),
        Collections.singletonList("/tmp"), "DC1");
    Map<PartitionId, Store> problematicStoreMap = new HashMap<>();
    PartitionId partitionId1 = new MockPartitionId(1, MockClusterMap.DEFAULT_PARTITION_CLASS,
        Collections.singletonList((MockDataNodeId) dataNodeId), 0);
    PartitionId partitionId2 = new MockPartitionId(2, MockClusterMap.DEFAULT_PARTITION_CLASS,
        Collections.singletonList((MockDataNodeId) dataNodeId), 0);
    problematicStoreMap.put(partitionId1, null);
    Map<StatsReportType, StatsSnapshot> snapshotsByType = new HashMap<>();
    snapshotsByType.put(StatsReportType.ACCOUNT_REPORT, new StatsSnapshot(0L, null));
    Store exceptionStore = new MockStore(new MockStoreStats(snapshotsByType, true));
    problematicStoreMap.put(partitionId2, exceptionStore);
    StatsManager testStatsManager = new StatsManager(new MockStorageManager(problematicStoreMap),
        Arrays.asList(partitionId1.getReplicaIds().get(0), partitionId2.getReplicaIds().get(0)), new MetricRegistry(),
        config, new MockTime());
    List<PartitionId> unreachablePartitions = new ArrayList<>();
    StatsSnapshot actualSnapshot = new StatsSnapshot(0L, null);
    for (PartitionId partitionId : problematicStoreMap.keySet()) {
      testStatsManager.collectAndAggregate(actualSnapshot, partitionId, unreachablePartitions);
    }
    assertEquals("Aggregated StatsSnapshot should not contain any value", 0L, actualSnapshot.getValue());
    assertEquals("Unreachable store count mismatch with expected value", 2, unreachablePartitions.size());
    String statsJSON = testStatsManager.getNodeStatsInJSON(StatsReportType.ACCOUNT_REPORT);
    StatsWrapper statsWrapper = mapper.readValue(statsJSON, StatsWrapper.class);
    List<String> unreachableStores = statsWrapper.getHeader().getUnreachableStores();
    assertTrue("The unreachable store list should contain Partition1 and Partition2",
        unreachableStores.containsAll(Arrays.asList(partitionId1.toPathString(), partitionId2.toPathString())));
    // test for the scenario where some stores are healthy and some are bad
    Map<PartitionId, Store> mixedStoreMap = new HashMap<>(storeMap);
    unreachablePartitions.clear();
    PartitionId partitionId3 = new MockPartitionId(3, MockClusterMap.DEFAULT_PARTITION_CLASS,
        Collections.singletonList((MockDataNodeId) dataNodeId), 0);
    PartitionId partitionId4 = new MockPartitionId(4, MockClusterMap.DEFAULT_PARTITION_CLASS,
        Collections.singletonList((MockDataNodeId) dataNodeId), 0);
    mixedStoreMap.put(partitionId3, null);
    mixedStoreMap.put(partitionId4, exceptionStore);
    testStatsManager = new StatsManager(new MockStorageManager(mixedStoreMap),
        Arrays.asList(partitionId3.getReplicaIds().get(0), partitionId4.getReplicaIds().get(0)), new MetricRegistry(),
        config, new MockTime());
    actualSnapshot = new StatsSnapshot(0L, null);
    for (PartitionId partitionId : mixedStoreMap.keySet()) {
      testStatsManager.collectAndAggregate(actualSnapshot, partitionId, unreachablePartitions);
    }
    assertTrue("Actual aggregated StatsSnapshot does not match with expected snapshot",
        preAggregatedSnapshot.equals(actualSnapshot));
    assertEquals("Unreachable store count mismatch with expected value", 2, unreachablePartitions.size());
    // test fetchSnapshot method in StatsManager
    unreachablePartitions.clear();
    // partition 0, 1, 2 are healthy stores, partition 3, 4 are bad ones.
    for (PartitionId partitionId : mixedStoreMap.keySet()) {
      StatsSnapshot snapshot =
          testStatsManager.fetchSnapshot(partitionId, unreachablePartitions, StatsReportType.ACCOUNT_REPORT);
      if (Integer.valueOf(partitionId.toPathString()) < 3) {
        assertTrue("Actual StatsSnapshot does not match with expected snapshot",
            snapshot.equals(partitionToSnapshot.get(partitionId)));
      }
    }
    assertEquals("Unreachable store count mismatch with expected value", 2, unreachablePartitions.size());
  }

  /**
   * Test to verify the {@link StatsManager} behaves correctly when dynamically adding/removing {@link ReplicaId}.
   * @throws StoreException
   * @throws IOException
   */
  @Test
  public void testAddAndRemoveReplica() throws StoreException, IOException {
    // setup testing environment
    Map<PartitionId, Store> testStoreMap = new HashMap<>();
    List<ReplicaId> testReplicas = new ArrayList<>();
    DataNodeId dataNodeId = new MockDataNodeId(Collections.singletonList(new Port(6667, PortType.PLAINTEXT)),
        Collections.singletonList("/tmp"), "DC1");
    Map<StatsReportType, StatsSnapshot> snapshotsByType = new HashMap<>();
    snapshotsByType.put(StatsReportType.ACCOUNT_REPORT, preAggregatedSnapshot);
    for (int i = 0; i < 3; i++) {
      PartitionId partitionId = new MockPartitionId(i, MockClusterMap.DEFAULT_PARTITION_CLASS,
          Collections.singletonList((MockDataNodeId) dataNodeId), 0);
      testStoreMap.put(partitionId, new MockStore(new MockStoreStats(snapshotsByType, false)));
      testReplicas.add(partitionId.getReplicaIds().get(0));
    }
    StorageManager mockStorageManager = new MockStorageManager(testStoreMap);
    StatsManager testStatsManager =
        new StatsManager(mockStorageManager, testReplicas, new MetricRegistry(), config, new MockTime());

    // verify that adding an existing store to StatsManager should fail
    assertFalse("Adding a store which already exists should fail", testStatsManager.addReplica(testReplicas.get(0)));
    PartitionId partitionId3 = new MockPartitionId(3, MockClusterMap.DEFAULT_PARTITION_CLASS,
        Collections.singletonList((MockDataNodeId) dataNodeId), 0);
    testStoreMap.put(partitionId3, new MockStore(new MockStoreStats(snapshotsByType, false)));
    // verify that partitionId3 is not in stats report before adding to statsManager
    String statsJSON = testStatsManager.getNodeStatsInJSON(StatsReportType.ACCOUNT_REPORT);
    StatsWrapper statsWrapper = mapper.readValue(statsJSON, StatsWrapper.class);
    assertFalse("Partition3 should not present in stats report",
        statsWrapper.getSnapshot().getSubMap().containsKey(partitionId3.toPathString()));
    // verify that after adding into statsManager, PartitionId3 is in stats report
    testStatsManager.addReplica(partitionId3.getReplicaIds().get(0));
    statsJSON = testStatsManager.getNodeStatsInJSON(StatsReportType.ACCOUNT_REPORT);
    statsWrapper = mapper.readValue(statsJSON, StatsWrapper.class);
    assertTrue("Partition3 should present in stats report",
        statsWrapper.getSnapshot().getSubMap().containsKey(partitionId3.toPathString()));
    // verify that after removing PartitionId0 (corresponding to the first replica in replicas list), PartitionId0 is not in the stats report
    PartitionId partitionId0 = testReplicas.get(0).getPartitionId();
    assertTrue("Partition0 should present in stats report before removal",
        statsWrapper.getSnapshot().getSubMap().containsKey(partitionId0.toPathString()));
    testStoreMap.remove(testReplicas.get(0).getPartitionId());
    testStatsManager.removeReplica(testReplicas.get(0));
    statsJSON = testStatsManager.getNodeStatsInJSON(StatsReportType.ACCOUNT_REPORT);
    statsWrapper = mapper.readValue(statsJSON, StatsWrapper.class);
    assertFalse("Partition0 should not present in stats report after removal",
        statsWrapper.getSnapshot().getSubMap().containsKey(partitionId0.toPathString()));
    // verify that removing the PartitionId0 should fail because it no longer exists in StatsManager
    assertFalse(testStatsManager.removeReplica(testReplicas.get(0)));

    // concurrent remove test
    CountDownLatch getStatsCountdown1 = new CountDownLatch(1);
    CountDownLatch waitRemoveCountdown = new CountDownLatch(1);
    ((MockStorageManager) mockStorageManager).waitOperationCountdown = waitRemoveCountdown;
    ((MockStorageManager) mockStorageManager).firstCall = true;
    ((MockStorageManager) mockStorageManager).unreachablePartitions.clear();
    for (Store store : testStoreMap.values()) {
      ((MockStore) store).getStatsCountdown = getStatsCountdown1;
      ((MockStore) store).isCollected = false;
    }
    List<PartitionId> partitionRemoved = new ArrayList<>();
    Utils.newThread(() -> {
      // wait until at least one store has been collected (this ensures stats aggregation using old snapshot of map)
      try {
        getStatsCountdown1.await();
      } catch (InterruptedException e) {
        throw new IllegalStateException("CountDown await was interrupted", e);
      }
      // find one store which hasn't been collected
      ReplicaId replicaToRemove = null;
      for (Map.Entry<PartitionId, Store> partitionToStore : testStoreMap.entrySet()) {
        MockStore store = (MockStore) partitionToStore.getValue();
        if (!store.isCollected) {
          replicaToRemove = partitionToStore.getKey().getReplicaIds().get(0);
          break;
        }
      }
      if (replicaToRemove != null) {
        testStatsManager.removeReplica(replicaToRemove);
        testStoreMap.remove(replicaToRemove.getPartitionId());
        partitionRemoved.add(replicaToRemove.getPartitionId());
        // count down to allow stats aggregation to proceed
        waitRemoveCountdown.countDown();
      }
    }, false).start();
    statsJSON = testStatsManager.getNodeStatsInJSON(StatsReportType.ACCOUNT_REPORT);
    statsWrapper = mapper.readValue(statsJSON, StatsWrapper.class);
    // verify that the removed store is indeed unreachable during stats aggregation
    assertTrue("The removed partition should be unreachable during aggregation",
        ((MockStorageManager) mockStorageManager).unreachablePartitions.contains(partitionRemoved.get(0)));
    // verify unreachable store list doesn't contain the store which is removed.
    List<String> unreachableStores = statsWrapper.getHeader().getUnreachableStores();
    assertFalse("The removed partition should not present in unreachable list",
        unreachableStores.contains(partitionRemoved.get(0).toPathString()));

    // concurrent add test
    CountDownLatch getStatsCountdown2 = new CountDownLatch(1);
    CountDownLatch waitAddCountdown = new CountDownLatch(1);
    ((MockStorageManager) mockStorageManager).waitOperationCountdown = waitAddCountdown;
    ((MockStorageManager) mockStorageManager).firstCall = true;
    ((MockStorageManager) mockStorageManager).unreachablePartitions.clear();
    for (Store store : testStoreMap.values()) {
      ((MockStore) store).getStatsCountdown = getStatsCountdown2;
      ((MockStore) store).isCollected = false;
    }
    PartitionId partitionId4 = new MockPartitionId(4, MockClusterMap.DEFAULT_PARTITION_CLASS,
        Collections.singletonList((MockDataNodeId) dataNodeId), 0);
    Utils.newThread(() -> {
      // wait until at least one store has been collected (this ensures stats aggregation using old snapshot of map)
      try {
        getStatsCountdown2.await();
      } catch (InterruptedException e) {
        throw new IllegalStateException("CountDown await was interrupted", e);
      }
      testStatsManager.addReplica(partitionId4.getReplicaIds().get(0));
      testStoreMap.put(partitionId4, new MockStore(new MockStoreStats(snapshotsByType, false)));
      // count down to allow stats aggregation to proceed
      waitAddCountdown.countDown();
    }, false).start();
    statsJSON = testStatsManager.getNodeStatsInJSON(StatsReportType.ACCOUNT_REPORT);
    statsWrapper = mapper.readValue(statsJSON, StatsWrapper.class);
    // verify that new added PartitionId4 is not in report for this round of aggregation
    assertFalse("Partition4 should not present in stats report",
        statsWrapper.getSnapshot().getSubMap().containsKey(partitionId4.toPathString()));
    // verify that new added PartitionId4 will be collected for next round of aggregation
    statsJSON = testStatsManager.getNodeStatsInJSON(StatsReportType.ACCOUNT_REPORT);
    statsWrapper = mapper.readValue(statsJSON, StatsWrapper.class);
    assertTrue("Partition4 should present in stats report",
        statsWrapper.getSnapshot().getSubMap().containsKey(partitionId4.toPathString()));
  }

  /**
   * Test that the {@link StatsManager} can correctly collect and aggregate all type of stats on the node. This
   * test is using randomly generated account snapshot and partitionClass snapshot in mock {@link StoreStats}.
   * @throws StoreException
   * @throws IOException
   */
  @Test
  public void testGetNodeStatsInJSON() throws IOException, StoreException {
    // initialize StatsManager and create all types of snapshots for testing
    List<ReplicaId> replicaIds = new ArrayList<>();
    PartitionId partitionId;
    DataNodeId dataNodeId;
    Map<PartitionId, Store> storeMap = new HashMap<>();
    List<StatsSnapshot> partitionClassSnapshots = new ArrayList<>();
    List<StatsSnapshot> accountSnapshots = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      dataNodeId = new MockDataNodeId(Collections.singletonList(new Port(6667, PortType.PLAINTEXT)),
          Collections.singletonList("/tmp"), "DC1");
      partitionId = new MockPartitionId(i,
          (i % 2 == 0) ? MockClusterMap.DEFAULT_PARTITION_CLASS : MockClusterMap.SPECIAL_PARTITION_CLASS,
          Collections.singletonList((MockDataNodeId) dataNodeId), 0);
      Map<StatsReportType, StatsSnapshot> allSnapshots = generateRandomSnapshot();
      partitionClassSnapshots.add(allSnapshots.get(StatsReportType.PARTITION_CLASS_REPORT));
      accountSnapshots.add(allSnapshots.get(StatsReportType.ACCOUNT_REPORT));
      storeMap.put(partitionId, new MockStore(new MockStoreStats(allSnapshots, false)));
      replicaIds.add(partitionId.getReplicaIds().get(0));
    }
    StorageManager storageManager = new MockStorageManager(storeMap);
    StatsManager statsManager =
        new StatsManager(storageManager, replicaIds, new MetricRegistry(), config, new MockTime());

    StatsSnapshot expectAccountSnapshot = new StatsSnapshot(0L, new HashMap<>());
    StatsSnapshot expectPartitionClassSnapshot = new StatsSnapshot(0L, new HashMap<>());
    for (int i = 0; i < accountSnapshots.size(); ++i) {
      Map<String, StatsSnapshot> partitionToAccountSnapshot = new HashMap<>();
      Map<String, StatsSnapshot> partitionToPartitionClassSnapshot = new HashMap<>();
      Map<String, StatsSnapshot> partitionClassSnapshotMap = new HashMap<>();
      String partitionIdStr = String.valueOf(i);
      String partitionClassStr =
          i % 2 == 0 ? MockClusterMap.DEFAULT_PARTITION_CLASS : MockClusterMap.SPECIAL_PARTITION_CLASS;
      partitionToAccountSnapshot.put(partitionIdStr, accountSnapshots.get(i));
      partitionToPartitionClassSnapshot.put(partitionIdStr, partitionClassSnapshots.get(i));
      partitionClassSnapshotMap.put(partitionClassStr,
          new StatsSnapshot(partitionClassSnapshots.get(i).getValue(), partitionToPartitionClassSnapshot));
      //aggregate two types of snapshots respectively
      StatsSnapshot.aggregate(expectAccountSnapshot,
          new StatsSnapshot(accountSnapshots.get(i).getValue(), partitionToAccountSnapshot));
      StatsSnapshot.aggregate(expectPartitionClassSnapshot,
          new StatsSnapshot(partitionClassSnapshots.get(i).getValue(), partitionClassSnapshotMap));
    }

    // Get node level stats in JSON to verify
    for (StatsReportType type : EnumSet.of(StatsReportType.ACCOUNT_REPORT, StatsReportType.PARTITION_CLASS_REPORT)) {
      String statsInJSON = statsManager.getNodeStatsInJSON(type);
      StatsSnapshot actualSnapshot = mapper.readValue(statsInJSON, StatsWrapper.class).getSnapshot();
      switch (type) {
        case ACCOUNT_REPORT:
          assertTrue("Mismatch in aggregated node stats at account level",
              expectAccountSnapshot.equals(actualSnapshot));
          break;
        case PARTITION_CLASS_REPORT:
          assertTrue("Mismatch in aggregated node stats at partitionClass level",
              expectPartitionClassSnapshot.equals(actualSnapshot));
          break;
        default:
          throw new IllegalArgumentException("Unrecognized stats report type: " + type);
      }
    }
  }

  /**
   * Test to verify {@link StatsManager} can start and shutdown properly.
   */
  @Test
  public void testStatsManagerStartAndShutdown() {
    statsManager.start();
    statsManager.shutdown();
  }

  /**
   * Test to verify {@link StatsManager} can shutdown properly even before it's started.
   */
  @Test
  public void testShutdownBeforeStart() {
    statsManager.shutdown();
  }

  /**
   * Generate a random, two levels of nesting (accountId, containerId) {@link StatsSnapshot} for testing aggregation
   * @return a map of all types of {@link StatsSnapshot} whose key is the type name and value is corresponding snapshot
   */
  private Map<StatsReportType, StatsSnapshot> generateRandomSnapshot() {
    Map<String, StatsSnapshot> accountMap = new HashMap<>();
    Map<String, StatsSnapshot> accountContainerPairMap = new HashMap<>();
    long totalSize = 0;
    for (int i = 0; i < random.nextInt(MAX_ACCOUNT_COUNT - MIN_ACCOUNT_COUNT + 1) + MIN_ACCOUNT_COUNT; i++) {
      String accountIdStr = "A[".concat(String.valueOf(i)).concat("]");
      Map<String, StatsSnapshot> containerMap = new HashMap<>();
      long subTotalSize = 0;
      for (int j = 0; j < random.nextInt(MAX_CONTAINER_COUNT - MIN_CONTAINER_COUNT + 1) + MIN_CONTAINER_COUNT; j++) {
        String containerIdStr = "C[".concat(String.valueOf(j)).concat("]");
        long validSize = random.nextInt(2501) + 500;
        subTotalSize += validSize;
        containerMap.put(containerIdStr, new StatsSnapshot(validSize, null));
        accountContainerPairMap.put(accountIdStr + Utils.ACCOUNT_CONTAINER_SEPARATOR + containerIdStr,
            new StatsSnapshot(validSize, null));
      }
      totalSize += subTotalSize;
      accountMap.put(accountIdStr, new StatsSnapshot(subTotalSize, containerMap));
    }
    Map<StatsReportType, StatsSnapshot> allSnapshots = new HashMap<>();
    allSnapshots.put(StatsReportType.PARTITION_CLASS_REPORT, new StatsSnapshot(totalSize, accountContainerPairMap));
    allSnapshots.put(StatsReportType.ACCOUNT_REPORT, new StatsSnapshot(totalSize, accountMap));
    return allSnapshots;
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
   * Mocked {@link Store} that is intended to return a predefined {@link StoreStats} when getStoreStats is called.
   */
  private class MockStore implements Store {
    private final StoreStats storeStats;
    CountDownLatch getStatsCountdown;
    boolean isCollected;

    MockStore(StoreStats storeStats) {
      this.storeStats = storeStats;
      getStatsCountdown = new CountDownLatch(0);
      isCollected = false;
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
      isCollected = true;
      getStatsCountdown.countDown();
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

    @Override
    public boolean isStarted() {
      throw new IllegalStateException("Not implemented");
    }
  }

  /**
   * Mocked {@link StoreStats} to return predefined {@link StatsSnapshot} when getStatsSnapshot is called.
   */
  private class MockStoreStats implements StoreStats {
    private final Map<StatsReportType, StatsSnapshot> snapshotsByType;
    private final boolean throwStoreException;

    MockStoreStats(Map<StatsReportType, StatsSnapshot> snapshotsByType, boolean throwStoreException) {
      this.snapshotsByType = snapshotsByType;
      this.throwStoreException = throwStoreException;
    }

    @Override
    public Pair<Long, Long> getValidSize(TimeRange timeRange) throws StoreException {
      throw new IllegalStateException("Not implemented");
    }

    @Override
    public Map<StatsReportType, StatsSnapshot> getStatsSnapshots(Set<StatsReportType> statsReportTypes,
        long referenceTimeInMs) throws StoreException {
      if (throwStoreException) {
        throw new StoreException("Test", StoreErrorCodes.Unknown_Error);
      }
      return snapshotsByType;
    }
  }
}
