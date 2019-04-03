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

package com.github.ambry.clustermap;

import com.github.ambry.server.StatsHeader;
import com.github.ambry.server.StatsReportType;
import com.github.ambry.server.StatsSnapshot;
import com.github.ambry.server.StatsWrapper;
import com.github.ambry.utils.Pair;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import static org.junit.Assert.*;


public class HelixClusterAggregatorTest {
  private static final long RELEVANT_PERIOD_IN_MINUTES = 60;
  private static final long DEFAULT_TIMESTAMP = 1000;
  private final String EXCEPTION_INSTANCE_NAME = "Exception_Instance";
  private final HelixClusterAggregator clusterAggregator;
  private final ObjectMapper mapper = new ObjectMapper();

  public HelixClusterAggregatorTest() {
    clusterAggregator = new HelixClusterAggregator(RELEVANT_PERIOD_IN_MINUTES);
  }

  /**
   * Basic tests to verify the cluster wide raw data and valid data aggregation. The tests also verify stats aggregation
   * for all types of stats reports.
   * @throws IOException
   */
  @Test
  public void testDoWorkBasic() throws IOException {
    int nodeCount = 3;
    Random random = new Random();
    // For each type of report, create snapshots for 3 stores with 3 accounts, 4 accounts and 5 accounts respectively.
    for (StatsReportType type : EnumSet.of(StatsReportType.ACCOUNT_REPORT, StatsReportType.PARTITION_CLASS_REPORT)) {
      List<StatsSnapshot> storeSnapshots = new ArrayList<>();
      for (int i = 3; i < 6; i++) {
        storeSnapshots.add(generateStoreStats(i, 3, random, type));
      }
      StatsWrapper nodeStats = generateNodeStats(storeSnapshots, DEFAULT_TIMESTAMP, type);
      String nodeStatsJSON = mapper.writeValueAsString(nodeStats);
      StatsWrapper emptyNodeStats = generateNodeStats(Collections.emptyList(), DEFAULT_TIMESTAMP, type);
      String emptyStatsJSON = mapper.writeValueAsString(emptyNodeStats);

      Map<String, String> instanceToStatsMap = new HashMap<>();
      // Each instance has exactly same nodeStatsJSON, the purpose is to ensure raw_data_size field and valid_data_size field
      // have different expected aggregated report.
      // For raw_data_size field, it simply sums up value from replicas of partition on all nodes even if they are exactly same;
      // For valid_data_size field, it filters out the replica(s) of certain partition within valid time range and
      // selects the replica with highest value.
      for (int i = 0; i < nodeCount; i++) {
        instanceToStatsMap.put("Instance_" + i, nodeStatsJSON);
      }
      // Add two special cases into instance-to-stats map for testing:
      // (1) empty stats report from certain instance
      // (2) corrupted/invalid stats report from certain instance (this is simulated by empty string)
      instanceToStatsMap.put("Instance_" + nodeCount, emptyStatsJSON);
      instanceToStatsMap.put(EXCEPTION_INSTANCE_NAME, "");

      // 1. Aggregate all snapshots into the first snapshot in snapshots list. The intention is to get expected aggregated snapshot.
      // 2. Then invoke clusterAggregator to do work on stats across all instances.
      // 3. Verify both raw stats and valid stats after aggregation
      Pair<String, String> aggregatedRawAndValidStats = clusterAggregator.doWork(instanceToStatsMap, type);

      StatsSnapshot expectedSnapshot = null;
      switch (type) {
        case ACCOUNT_REPORT:
          // Since all nodes have exactly same statsSnapshot, aggregated snapshot of storeSnapshots list on single node is what
          // we expect for valid data aggregation.
          for (int i = 1; i < storeSnapshots.size(); i++) {
            StatsSnapshot.aggregate(storeSnapshots.get(0), storeSnapshots.get(i));
          }
          expectedSnapshot = storeSnapshots.get(0);
          break;
        case PARTITION_CLASS_REPORT:
          // Invoke reduceByPartitionClass to remove partition level and only keep the partition class and account_container entries
          expectedSnapshot = HelixClusterAggregator.reduceByPartitionClass(nodeStats.getSnapshot());
          break;
      }

      // Verify cluster wide raw stats aggregation
      StatsSnapshot rawSnapshot = mapper.readValue(aggregatedRawAndValidStats.getFirst(), StatsSnapshot.class);
      assertEquals("Mismatch in total value of " + type, nodeCount * expectedSnapshot.getValue(),
          rawSnapshot.getValue());
      if (type == StatsReportType.ACCOUNT_REPORT) {
        verifyAggregatedRawStatsForAccountReport(rawSnapshot, expectedSnapshot, nodeCount);
      } else if (type == StatsReportType.PARTITION_CLASS_REPORT) {
        verifyAggregatedRawStatsForPartitionClassReport(rawSnapshot, expectedSnapshot, nodeCount);
      }

      // Verify cluster wide stats aggregation
      StatsSnapshot actualSnapshot = mapper.readValue(aggregatedRawAndValidStats.getSecond(), StatsSnapshot.class);
      assertTrue("Mismatch in the aggregated snapshot", expectedSnapshot.equals(actualSnapshot));
      // Verify aggregator keeps track of instances where exception occurred.
      assertEquals("Mismatch in instances where exception occurred", Collections.singletonList(EXCEPTION_INSTANCE_NAME),
          clusterAggregator.getExceptionOccurredInstances(type));
    }
  }

  /**
   * Test stats aggregation with different number of stores on different nodes.
   * Only used for partitionClass aggregation testing.
   * @throws IOException
   */
  @Test
  public void testDoWorkWithDiffNumberOfStores() throws IOException {
    List<StatsSnapshot> storeSnapshots1 = new ArrayList<>();
    List<StatsSnapshot> storeSnapshots2 = new ArrayList<>();
    List<StatsSnapshot> storeSnapshots2Copy = new ArrayList<>();
    int seed = 1111;
    // storeSnapshots1 only has 2 store stats. storeSnapshots2 and storeSnapshots2Copy have 3 store stats each.
    for (int i = 3; i < 6; i++) {
      if (i < 5) {
        storeSnapshots1.add(generateStoreStats(i, 3, new Random(seed), StatsReportType.PARTITION_CLASS_REPORT));
      }
      storeSnapshots2.add(generateStoreStats(i, 3, new Random(seed), StatsReportType.PARTITION_CLASS_REPORT));
      storeSnapshots2Copy.add(generateStoreStats(i, 3, new Random(seed), StatsReportType.PARTITION_CLASS_REPORT));
    }
    StatsWrapper nodeStatsWrapper1 =
        generateNodeStats(storeSnapshots1, DEFAULT_TIMESTAMP, StatsReportType.PARTITION_CLASS_REPORT);
    StatsWrapper nodeStatsWrapper2 =
        generateNodeStats(storeSnapshots2, DEFAULT_TIMESTAMP, StatsReportType.PARTITION_CLASS_REPORT);
    StatsWrapper nodeStatsWrapper2Copy =
        generateNodeStats(storeSnapshots2Copy, DEFAULT_TIMESTAMP, StatsReportType.PARTITION_CLASS_REPORT);
    Map<String, String> instanceStatsMap = new LinkedHashMap<>();
    instanceStatsMap.put("Instance_1", mapper.writeValueAsString(nodeStatsWrapper1));
    instanceStatsMap.put("Instance_2", mapper.writeValueAsString(nodeStatsWrapper2));

    Pair<String, String> aggregatedRawAndValidStats =
        clusterAggregator.doWork(instanceStatsMap, StatsReportType.PARTITION_CLASS_REPORT);

    // verify aggregation on raw data
    StatsSnapshot expectedRawSnapshot = new StatsSnapshot(0L, null);
    StatsSnapshot.aggregate(expectedRawSnapshot, nodeStatsWrapper1.getSnapshot());
    StatsSnapshot.aggregate(expectedRawSnapshot, nodeStatsWrapper2Copy.getSnapshot());
    expectedRawSnapshot = HelixClusterAggregator.reduceByPartitionClass(expectedRawSnapshot);
    StatsSnapshot rawSnapshot = mapper.readValue(aggregatedRawAndValidStats.getFirst(), StatsSnapshot.class);
    assertTrue("Mismatch in the raw data aggregated snapshot", expectedRawSnapshot.equals(rawSnapshot));

    // verify aggregation on valid data
    StatsSnapshot expectedValidsnapshot =
        HelixClusterAggregator.reduceByPartitionClass(nodeStatsWrapper2.getSnapshot());
    StatsSnapshot validSnapshot = mapper.readValue(aggregatedRawAndValidStats.getSecond(), StatsSnapshot.class);
    assertTrue("Mismatch in the valid data aggregated snapshot", expectedValidsnapshot.equals(validSnapshot));
  }

  /**
   * Tests to verify cluster wide aggregation with outdated node stats.
   * @throws IOException
   */
  @Test
  public void testDoWorkWithOutdatedNode() throws IOException {
    long seed = 1111;
    for (StatsReportType type : EnumSet.of(StatsReportType.ACCOUNT_REPORT, StatsReportType.PARTITION_CLASS_REPORT)) {
      List<StatsSnapshot> upToDateStoreSnapshots = new ArrayList<>();
      List<StatsSnapshot> outdatedStoreSnapshots = new ArrayList<>();
      upToDateStoreSnapshots.add(generateStoreStats(5, 3, new Random(seed), type));
      outdatedStoreSnapshots.add(generateStoreStats(6, 3, new Random(seed), type));
      StatsWrapper upToDateNodeStats =
          generateNodeStats(upToDateStoreSnapshots, TimeUnit.MINUTES.toMillis(2 * RELEVANT_PERIOD_IN_MINUTES), type);
      StatsWrapper outdatedNodeStats = generateNodeStats(outdatedStoreSnapshots, 0, type);
      StatsWrapper emptyNodeStats =
          generateNodeStats(Collections.emptyList(), TimeUnit.MINUTES.toMillis(2 * RELEVANT_PERIOD_IN_MINUTES), type);
      Map<String, String> instanceToStatsMap = new LinkedHashMap<>();
      instanceToStatsMap.put("Instance_0", mapper.writeValueAsString(outdatedNodeStats));
      instanceToStatsMap.put("Instance_1", mapper.writeValueAsString(upToDateNodeStats));
      instanceToStatsMap.put("Instance_2", mapper.writeValueAsString(emptyNodeStats));
      instanceToStatsMap.put(EXCEPTION_INSTANCE_NAME, "");
      Pair<String, String> aggregatedRawAndValidStats = clusterAggregator.doWork(instanceToStatsMap, type);
      StatsSnapshot expectedValidSnapshot = null;
      StatsSnapshot expectedRawSnapshot = new StatsSnapshot(0L, new HashMap<>());
      switch (type) {
        case ACCOUNT_REPORT:
          expectedValidSnapshot = upToDateStoreSnapshots.get(0);
          StatsSnapshot.aggregate(expectedRawSnapshot, outdatedStoreSnapshots.get(0));
          StatsSnapshot.aggregate(expectedRawSnapshot, upToDateStoreSnapshots.get(0));
          break;
        case PARTITION_CLASS_REPORT:
          expectedValidSnapshot = HelixClusterAggregator.reduceByPartitionClass(upToDateNodeStats.getSnapshot());
          StatsSnapshot.aggregate(expectedRawSnapshot, outdatedNodeStats.getSnapshot());
          StatsSnapshot.aggregate(expectedRawSnapshot, upToDateNodeStats.getSnapshot());
          expectedRawSnapshot = HelixClusterAggregator.reduceByPartitionClass(expectedRawSnapshot);
          break;
      }

      // verify cluster wide aggregation on raw stats with outdated node stats
      StatsSnapshot rawSnapshot = mapper.readValue(aggregatedRawAndValidStats.getFirst(), StatsSnapshot.class);
      assertTrue("Mismatch in the aggregated raw snapshot", expectedRawSnapshot.equals(rawSnapshot));

      // verify cluster wide aggregation on valid stats with outdated node stats
      StatsSnapshot actualSnapshot = mapper.readValue(aggregatedRawAndValidStats.getSecond(), StatsSnapshot.class);
      assertTrue("Mismatch in the aggregated valid snapshot", expectedValidSnapshot.equals(actualSnapshot));

      // verify aggregator keeps track of instances where exception occurred.
      assertEquals("Mismatch in instances where exception occurred", Collections.singletonList(EXCEPTION_INSTANCE_NAME),
          clusterAggregator.getExceptionOccurredInstances(type));
    }
  }

  /**
   * Tests to verify cluster aggregation with node stats that contain different partition stats.
   * @throws IOException
   */
  @Test
  public void testDoWorkWithDiffNodeStats() throws IOException {
    long seed = 1234;
    for (StatsReportType type : EnumSet.of(StatsReportType.ACCOUNT_REPORT, StatsReportType.PARTITION_CLASS_REPORT)) {
      List<StatsSnapshot> greaterStoreSnapshots = new ArrayList<>();
      List<StatsSnapshot> smallerStoreSnapshots = new ArrayList<>();
      List<StatsSnapshot> mediumStoreSnapshots = new ArrayList<>();
      greaterStoreSnapshots.add(generateStoreStats(6, 3, new Random(seed), type));
      mediumStoreSnapshots.add(generateStoreStats(5, 3, new Random(seed), type));
      smallerStoreSnapshots.add(generateStoreStats(5, 3, new Random(seed), type));
      StatsWrapper greaterNodeStats = generateNodeStats(greaterStoreSnapshots, DEFAULT_TIMESTAMP, type);
      StatsWrapper mediumNodeStats = generateNodeStats(mediumStoreSnapshots, DEFAULT_TIMESTAMP, type);
      StatsWrapper smallerNodeStats = generateNodeStats(smallerStoreSnapshots, DEFAULT_TIMESTAMP, type);
      StatsWrapper emptyNodeStats = generateNodeStats(Collections.emptyList(), DEFAULT_TIMESTAMP, type);
      Map<String, String> instanceToStatsMap = new LinkedHashMap<>();
      instanceToStatsMap.put("Instance_0", mapper.writeValueAsString(smallerNodeStats));
      instanceToStatsMap.put("Instance_1", mapper.writeValueAsString(greaterNodeStats));
      instanceToStatsMap.put("Instance_2", mapper.writeValueAsString(mediumNodeStats));
      instanceToStatsMap.put("Instance_3", mapper.writeValueAsString(emptyNodeStats));
      instanceToStatsMap.put(EXCEPTION_INSTANCE_NAME, "");
      Pair<String, String> aggregatedRawAndValidStats = clusterAggregator.doWork(instanceToStatsMap, type);

      StatsSnapshot expectedRawSnapshot = new StatsSnapshot(0L, new HashMap<>());
      StatsSnapshot expectedValidSnapshot = null;
      switch (type) {
        case ACCOUNT_REPORT:
          StatsSnapshot.aggregate(expectedRawSnapshot, smallerStoreSnapshots.get(0));
          StatsSnapshot.aggregate(expectedRawSnapshot, mediumStoreSnapshots.get(0));
          StatsSnapshot.aggregate(expectedRawSnapshot, greaterStoreSnapshots.get(0));
          expectedValidSnapshot = greaterStoreSnapshots.get(0);
          break;
        case PARTITION_CLASS_REPORT:
          expectedValidSnapshot = HelixClusterAggregator.reduceByPartitionClass(greaterNodeStats.getSnapshot());
          StatsSnapshot.aggregate(expectedRawSnapshot, mediumNodeStats.getSnapshot());
          StatsSnapshot.aggregate(expectedRawSnapshot, smallerNodeStats.getSnapshot());
          StatsSnapshot.aggregate(expectedRawSnapshot, greaterNodeStats.getSnapshot());
          expectedRawSnapshot = HelixClusterAggregator.reduceByPartitionClass(expectedRawSnapshot);
          break;
      }

      // verify cluster wide aggregation on raw data with different node stats
      StatsSnapshot rawSnapshot = mapper.readValue(aggregatedRawAndValidStats.getFirst(), StatsSnapshot.class);
      assertTrue("Mismatch in the raw aggregated snapshot for " + type, expectedRawSnapshot.equals(rawSnapshot));

      // verify cluster wide aggregation on valid data with different node stats
      StatsSnapshot actualSnapshot = mapper.readValue(aggregatedRawAndValidStats.getSecond(), StatsSnapshot.class);
      assertTrue("Mismatch in the valid aggregated snapshot for " + type, expectedValidSnapshot.equals(actualSnapshot));

      // verify aggregator keeps track of instances where exception occurred.
      assertEquals("Mismatch in instances where exception occurred", Collections.singletonList(EXCEPTION_INSTANCE_NAME),
          clusterAggregator.getExceptionOccurredInstances(type));
    }
  }

  /**
   * Tests to verify cluster aggregation with all empty nodes.
   * @throws IOException
   */
  @Test
  public void testStatsAggregationWithAllEmptyNodes() throws IOException {
    int nodeCount = 3;
    for (StatsReportType type : EnumSet.of(StatsReportType.ACCOUNT_REPORT, StatsReportType.PARTITION_CLASS_REPORT)) {
      StatsWrapper emptyNodeStats = generateNodeStats(Collections.emptyList(), DEFAULT_TIMESTAMP, type);
      String emptyStatsJSON = mapper.writeValueAsString(emptyNodeStats);
      Map<String, String> instanceToStatsMap = new HashMap<>();
      for (int i = 0; i < nodeCount; i++) {
        instanceToStatsMap.put("Instance_" + i, emptyStatsJSON);
      }
      Pair<String, String> aggregatedRawAndValidStats = clusterAggregator.doWork(instanceToStatsMap, type);
      StatsSnapshot expectedSnapshot = new StatsSnapshot(0L, null);
      StatsSnapshot rawSnapshot = mapper.readValue(aggregatedRawAndValidStats.getFirst(), StatsSnapshot.class);
      StatsSnapshot validSnapshot = mapper.readValue(aggregatedRawAndValidStats.getSecond(), StatsSnapshot.class);
      assertTrue("Mismatch in raw snapshot", expectedSnapshot.equals(rawSnapshot));
      assertTrue("Mismatch in valid snapshot", expectedSnapshot.equals(validSnapshot));
    }
  }

  /**
   * Given a {@link List} of {@link StatsSnapshot}s and a timestamp generate a {@link StatsWrapper} that would have been
   * produced by a node.
   * @param storeSnapshots a {@link List} of store level {@link StatsSnapshot}s.
   * @param timestamp the timestamp to be attached to the generated {@link StatsWrapper}
   * @param type the type of stats report to generate on this node
   * @return the generated node level {@link StatsWrapper}
   */
  private StatsWrapper generateNodeStats(List<StatsSnapshot> storeSnapshots, long timestamp, StatsReportType type) {
    long total = 0;
    int numbOfPartitions = storeSnapshots.size();
    Map<String, StatsSnapshot> partitionMap = new HashMap<>();
    Map<String, StatsSnapshot> partitionClassMap = new HashMap<>();
    String[] PARTITION_CLASS = new String[]{"PartitionClass1", "PartitionClass2"};
    for (int i = 0; i < numbOfPartitions; i++) {
      String partitionIdStr = "Partition[" + i + "]";
      StatsSnapshot partitionSnapshot = storeSnapshots.get(i);
      partitionMap.put(partitionIdStr, partitionSnapshot);
      total += partitionSnapshot.getValue();
      if (type == StatsReportType.PARTITION_CLASS_REPORT) {
        String partitionClassStr = PARTITION_CLASS[i % PARTITION_CLASS.length];
        StatsSnapshot partitionClassSnapshot =
            partitionClassMap.getOrDefault(partitionClassStr, new StatsSnapshot(0L, new HashMap<>()));
        partitionClassSnapshot.setValue(partitionClassSnapshot.getValue() + partitionSnapshot.getValue());
        partitionClassSnapshot.getSubMap().put(partitionIdStr, partitionSnapshot);
        partitionClassMap.put(partitionClassStr, partitionClassSnapshot);
      }
    }
    StatsSnapshot nodeSnapshot = null;
    if (type == StatsReportType.ACCOUNT_REPORT) {
      nodeSnapshot = new StatsSnapshot(total, partitionMap);
    } else if (type == StatsReportType.PARTITION_CLASS_REPORT) {
      nodeSnapshot = new StatsSnapshot(total, partitionClassMap);
    }
    StatsHeader header =
        new StatsHeader(StatsHeader.StatsDescription.STORED_DATA_SIZE, timestamp, numbOfPartitions, numbOfPartitions,
            Collections.emptyList());
    return new StatsWrapper(header, nodeSnapshot);
  }

  /**
   * Generate certain type of {@link StatsSnapshot} based on the given parameters that would have been produced by a
   * {@link com.github.ambry.store.Store}.
   * @param accountCount number of account entry in the {@link StatsSnapshot}
   * @param containerCount number of container entry in the {@link StatsSnapshot}
   * @param random the random generator to be used
   * @param type the type of stats report to be generated for the store
   * @return the generated store level {@link StatsSnapshot}
   */
  private StatsSnapshot generateStoreStats(int accountCount, int containerCount, Random random, StatsReportType type) {
    Map<String, StatsSnapshot> subMap = new HashMap<>();
    long totalSize = 0;
    for (int i = 0; i < accountCount; i++) {
      String accountIdStr = "Account[" + i + "]";
      Map<String, StatsSnapshot> containerMap = new HashMap<>();
      long subTotalSize = 0;
      for (int j = 0; j < containerCount; j++) {
        String containerIdStr = "Container[" + j + "]";
        long validSize = random.nextInt(2501) + 500;
        subTotalSize += validSize;
        if (type == StatsReportType.ACCOUNT_REPORT) {
          containerMap.put(containerIdStr, new StatsSnapshot(validSize, null));
        } else if (type == StatsReportType.PARTITION_CLASS_REPORT) {
          subMap.put(accountIdStr + "_" + containerIdStr, new StatsSnapshot(validSize, null));
        }
      }
      totalSize += subTotalSize;
      if (type == StatsReportType.ACCOUNT_REPORT) {
        subMap.put(accountIdStr, new StatsSnapshot(subTotalSize, containerMap));
      }
    }
    return new StatsSnapshot(totalSize, subMap);
  }

  /**
   * Help verify the aggregated raw stats match expected stats snapshot for {@link StatsReportType#ACCOUNT_REPORT}
   * @param rawSnapshot the raw stats snapshot after aggregation
   * @param expectedSnapshot the expected stats snapshot
   * @param nodeCount total number of instances involved in stats aggregation
   */
  private void verifyAggregatedRawStatsForAccountReport(StatsSnapshot rawSnapshot, StatsSnapshot expectedSnapshot,
      int nodeCount) {
    Map<String, StatsSnapshot> rawAccountMap = rawSnapshot.getSubMap();
    assertEquals("Mismatch in number of accounts", expectedSnapshot.getSubMap().size(), rawAccountMap.size());
    for (Map.Entry<String, StatsSnapshot> accountEntry : expectedSnapshot.getSubMap().entrySet()) {
      assertTrue("Expected account entry not found in the raw aggregated snapshot",
          rawAccountMap.containsKey(accountEntry.getKey()));
      assertEquals("Mismatch in account value", nodeCount * accountEntry.getValue().getValue(),
          rawAccountMap.get(accountEntry.getKey()).getValue());
      Map<String, StatsSnapshot> rawContainerMap = rawAccountMap.get(accountEntry.getKey()).getSubMap();
      assertEquals("Mismatch in number of accounts", accountEntry.getValue().getSubMap().size(),
          rawContainerMap.size());
      for (Map.Entry<String, StatsSnapshot> containerEntry : accountEntry.getValue().getSubMap().entrySet()) {
        assertTrue("Expected container entry not found in the raw aggregated snapshot",
            rawContainerMap.containsKey(containerEntry.getKey()));
        assertEquals("Mismatch in container value", nodeCount * containerEntry.getValue().getValue(),
            rawContainerMap.get(containerEntry.getKey()).getValue());
      }
    }
  }

  /**
   * Help verify the aggregated raw stats match expected stats snapshot for {@link StatsReportType#PARTITION_CLASS_REPORT}
   * @param rawSnapshot the raw stats snapshot after aggregation
   * @param expectedSnapshot the expected stats snapshot
   * @param nodeCount total number of instances involved in stats aggregation
   */
  private void verifyAggregatedRawStatsForPartitionClassReport(StatsSnapshot rawSnapshot,
      StatsSnapshot expectedSnapshot, int nodeCount) {
    assertEquals("Mismatch in number of partition classes", expectedSnapshot.getSubMap().size(),
        rawSnapshot.getSubMap().size());
    Map<String, StatsSnapshot> rawPartitionClassMap = rawSnapshot.getSubMap();
    Map<String, StatsSnapshot> expectedSnapshotOnOneNode = expectedSnapshot.getSubMap();
    for (Map.Entry<String, StatsSnapshot> partitionClassEntry : rawPartitionClassMap.entrySet()) {
      String partitionClassId = partitionClassEntry.getKey();
      assertEquals("Mismatch in value of partition class: " + partitionClassId,
          nodeCount * expectedSnapshotOnOneNode.get(partitionClassId).getValue(),
          partitionClassEntry.getValue().getValue());
      Map<String, StatsSnapshot> nodeContainerMap = expectedSnapshotOnOneNode.get(partitionClassId).getSubMap();
      assertEquals("Mismatch in number of containers in partition class: " + partitionClassId, nodeContainerMap.size(),
          partitionClassEntry.getValue().getSubMap().size());
      for (Map.Entry<String, StatsSnapshot> containerEntry : partitionClassEntry.getValue().getSubMap().entrySet()) {
        String containerIdStr = containerEntry.getKey();
        assertEquals("Mismatch in value of container: " + containerIdStr,
            nodeCount * nodeContainerMap.get(containerIdStr).getValue(), containerEntry.getValue().getValue());
      }
    }
  }
}
