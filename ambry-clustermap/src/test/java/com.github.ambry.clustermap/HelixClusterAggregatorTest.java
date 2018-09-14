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
   * Basic tests to verify the cluster wide raw data and valid data aggregation. The tests are performed to verify stats
   * aggregation for both account aggregated report and partition class aggregated report.
   * @throws IOException
   */
  @Test
  public void testDoWorkBasic() throws IOException {
    int nodeCount = 3;
    Random random = new Random();
    List<StatsSnapshot> storeSnapshotsForAccount = new ArrayList<>();
    List<StatsSnapshot> storeSnapshotsForPartitionClass = new ArrayList<>();
    // create snapshots for 3 stores with 3 accounts, 4 accounts and 5 accounts respectively.
    for (int i = 3; i < 6; i++) {
      storeSnapshotsForAccount.add(generateStoreStats(i, 3, random, StatsReportType.ACCOUNT_REPORT));
      storeSnapshotsForPartitionClass.add(generateStoreStats(i, 3, random, StatsReportType.PARTITION_CLASS_REPORT));
    }
    StatsWrapper nodeStatsForAccount =
        generateNodeStats(storeSnapshotsForAccount, DEFAULT_TIMESTAMP, StatsReportType.ACCOUNT_REPORT);
    StatsWrapper nodeStatsForPartitionClass =
        generateNodeStats(storeSnapshotsForPartitionClass, DEFAULT_TIMESTAMP, StatsReportType.PARTITION_CLASS_REPORT);
    StatsWrapper emptyNodeForAccount =
        generateNodeStats(Collections.EMPTY_LIST, DEFAULT_TIMESTAMP, StatsReportType.ACCOUNT_REPORT);
    StatsWrapper emptyNodeForPartitionClass =
        generateNodeStats(Collections.EMPTY_LIST, DEFAULT_TIMESTAMP, StatsReportType.PARTITION_CLASS_REPORT);

    String nodeStatsJSONForAccount = mapper.writeValueAsString(nodeStatsForAccount);
    String nodeStatsJSONForPartitionClass = mapper.writeValueAsString(nodeStatsForPartitionClass);
    String emptyJSONForAccount = mapper.writeValueAsString(emptyNodeForAccount);
    String emptyJSONForPartitionClass = mapper.writeValueAsString(emptyNodeForPartitionClass);
    Map<String, String> instanceStatsForAccount = new HashMap<>();
    Map<String, String> instanceStatsForPartitionClass = new HashMap<>();
    // Each node has exactly same nodeStatsJSON, the purpose is to ensure raw_data_size field and valid_data_size field
    // have different expected aggregated report.
    // For raw_data_size field, it simply sums up value from replicas of partition on all nodes even if they are exactly same;
    // For valid_data_size field, it filters out the replica(s) of a specific partition within valid time range and
    // selects the replica with highest value.
    for (int i = 0; i < nodeCount; i++) {
      instanceStatsForAccount.put("Instance_" + i, (nodeStatsJSONForAccount));
      instanceStatsForPartitionClass.put("Instance_" + i, (nodeStatsJSONForPartitionClass));
    }
    // Add two special cases into instance-to-stats map for testing:
    // (1) empty stats report from certain instance
    // (2) corrupted/invalid stats report from certain instance (this is simulated by empty string)
    instanceStatsForAccount.put("Instance_" + nodeCount, emptyJSONForAccount);
    instanceStatsForPartitionClass.put("Instance_" + nodeCount, emptyJSONForPartitionClass);
    instanceStatsForAccount.put(EXCEPTION_INSTANCE_NAME, "");
    instanceStatsForPartitionClass.put(EXCEPTION_INSTANCE_NAME, "");
    //aggregate all snapshots into the first snapshot in storeSnapshots list
    for (int i = 1; i < storeSnapshotsForAccount.size(); i++) {
      StatsSnapshot.aggregate(storeSnapshotsForAccount.get(0), storeSnapshotsForAccount.get(i));
    }
    Pair<String, String> resultsForAccount =
        clusterAggregator.doWork(instanceStatsForAccount, StatsReportType.ACCOUNT_REPORT);
    Pair<String, String> resultsForPartitionClass =
        clusterAggregator.doWork(instanceStatsForPartitionClass, StatsReportType.PARTITION_CLASS_REPORT);
    // since all nodes have exactly same statsSnapshot, aggregated snapshot of storeSnapshots list on single node is what
    // we expect for valid data aggregation.
    StatsSnapshot expectedAccountSnapshot = storeSnapshotsForAccount.get(0);
    StatsSnapshot expectedPartitionClassSnapshot =
        HelixClusterAggregator.reduceByPartitionClass(nodeStatsForPartitionClass.getSnapshot());

    // verify cluster-wide aggregation on raw data
    StatsSnapshot rawSnapshotForAccount = mapper.readValue(resultsForAccount.getFirst(), StatsSnapshot.class);
    StatsSnapshot rawSnapshotForPartitionClass =
        mapper.readValue(resultsForPartitionClass.getFirst(), StatsSnapshot.class);

    assertEquals("Mismatch in total value of account report", nodeCount * expectedAccountSnapshot.getValue(),
        rawSnapshotForAccount.getValue());
    Map<String, StatsSnapshot> rawAccountMap = rawSnapshotForAccount.getSubMap();
    assertEquals("Mismatch in number of accounts", expectedAccountSnapshot.getSubMap().size(), rawAccountMap.size());
    for (Map.Entry<String, StatsSnapshot> accountEntry : expectedAccountSnapshot.getSubMap().entrySet()) {
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

    assertEquals("Mismatch in total value of partitionClass report",
        nodeCount * nodeStatsForPartitionClass.getSnapshot().getValue(), rawSnapshotForPartitionClass.getValue());
    assertEquals("Mismatch in number of partition classes", nodeStatsForPartitionClass.getSnapshot().getSubMap().size(),
        rawSnapshotForPartitionClass.getSubMap().size());
    Map<String, StatsSnapshot> rawPartitionClassMap = rawSnapshotForPartitionClass.getSubMap();
    Map<String, StatsSnapshot> nodePartitionClassMap =
        expectedPartitionClassSnapshot.getSubMap();//nodeStatsForPartitionClass.getSnapshot().getSubMap();
    for (Map.Entry<String, StatsSnapshot> partitionClassEntry : rawPartitionClassMap.entrySet()) {
      String partitionClassId = partitionClassEntry.getKey();
      Map<String, StatsSnapshot> nodeContainerMap = nodePartitionClassMap.get(partitionClassId).getSubMap();
      assertEquals("Mismatch in value of partition class: " + partitionClassId,
          nodeCount * nodePartitionClassMap.get(partitionClassId).getValue(),
          partitionClassEntry.getValue().getValue());
      assertEquals("Mismatch in number of containers in partition class: " + partitionClassId, nodeContainerMap.size(),
          partitionClassEntry.getValue().getSubMap().size());
      for (Map.Entry<String, StatsSnapshot> containerEntry : partitionClassEntry.getValue().getSubMap().entrySet()) {
        String containerIdStr = containerEntry.getKey();
        assertEquals("Mismatch in value of container: " + containerIdStr,
            nodeCount * nodeContainerMap.get(containerIdStr).getValue(), containerEntry.getValue().getValue());
      }
    }

    // verify that cluster wide aggregation on valid data obeys two rules: (1) within valid time range; (2) selects replica with highest value
    StatsSnapshot validSnapshotForAccount = mapper.readValue(resultsForAccount.getSecond(), StatsSnapshot.class);
    assertTrue("Mismatch in the aggregated snapshot with valid time range at account level",
        expectedAccountSnapshot.equals(validSnapshotForAccount));
    StatsSnapshot validSnapshotForPartitionClass =
        mapper.readValue(resultsForPartitionClass.getSecond(), StatsSnapshot.class);
    assertTrue("Mismatch in the aggregated snapshot with valid time range at partitionClass level",
        expectedPartitionClassSnapshot.equals(validSnapshotForPartitionClass));
    // verify aggregator keeps track of instances where exception occurred.
    for (StatsReportType type : EnumSet.allOf(StatsReportType.class)) {
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

    Pair<String, String> results = clusterAggregator.doWork(instanceStatsMap, StatsReportType.PARTITION_CLASS_REPORT);

    // verify aggregation on raw data
    StatsSnapshot expectedRawSnapshot = new StatsSnapshot(0L, null);
    StatsSnapshot.aggregate(expectedRawSnapshot, nodeStatsWrapper1.getSnapshot());
    StatsSnapshot.aggregate(expectedRawSnapshot, nodeStatsWrapper2Copy.getSnapshot());
    expectedRawSnapshot = HelixClusterAggregator.reduceByPartitionClass(expectedRawSnapshot);
    StatsSnapshot rawSnapshot = mapper.readValue(results.getFirst(), StatsSnapshot.class);
    assertTrue("Mismatch in the raw data aggregated snapshot", expectedRawSnapshot.equals(rawSnapshot));

    // verify aggregation on valid data
    StatsSnapshot expectedValidsnapshot =
        HelixClusterAggregator.reduceByPartitionClass(nodeStatsWrapper2.getSnapshot());
    StatsSnapshot validSnapshot = mapper.readValue(results.getSecond(), StatsSnapshot.class);
    assertTrue("Mismatch in the valid data aggregated snapshot", expectedValidsnapshot.equals(validSnapshot));
  }

  /**
   * Tests to verify cluster wide aggregation with outdated node stats for account stats report.
   * @throws IOException
   */
  @Test
  public void testDoWorkWithOutdatedNodeForAccount() throws IOException {
    long seed = 1111;
    List<StatsSnapshot> upToDateStoreSnapshots = new ArrayList<>();
    List<StatsSnapshot> outdatedStoreSnapshots = new ArrayList<>();
    upToDateStoreSnapshots.add(generateStoreStats(5, 3, new Random(seed), StatsReportType.ACCOUNT_REPORT));
    outdatedStoreSnapshots.add(generateStoreStats(6, 3, new Random(seed), StatsReportType.ACCOUNT_REPORT));
    StatsWrapper upToDateNodeStats =
        generateNodeStats(upToDateStoreSnapshots, TimeUnit.MINUTES.toMillis(2 * RELEVANT_PERIOD_IN_MINUTES),
            StatsReportType.ACCOUNT_REPORT);
    StatsWrapper outdatedNodeStats = generateNodeStats(outdatedStoreSnapshots, 0, StatsReportType.ACCOUNT_REPORT);
    StatsWrapper emptyNodeStats =
        generateNodeStats(Collections.EMPTY_LIST, TimeUnit.MINUTES.toMillis(2 * RELEVANT_PERIOD_IN_MINUTES),
            StatsReportType.ACCOUNT_REPORT);
    Map<String, String> instanceStatsMap = new LinkedHashMap<>();
    instanceStatsMap.put("Instance_0", mapper.writeValueAsString(outdatedNodeStats));
    instanceStatsMap.put("Instance_1", mapper.writeValueAsString(upToDateNodeStats));
    instanceStatsMap.put("Instance_2", mapper.writeValueAsString(emptyNodeStats));
    instanceStatsMap.put(EXCEPTION_INSTANCE_NAME, "");
    Pair<String, String> results = clusterAggregator.doWork(instanceStatsMap, StatsReportType.ACCOUNT_REPORT);
    StatsSnapshot expectedSnapshot = upToDateStoreSnapshots.get(0);
    // verify cluster wide aggregation on valid data with outdated node stats
    StatsSnapshot actualSnapshot = mapper.readValue(results.getSecond(), StatsSnapshot.class);
    assertTrue("Mismatch in the valid data aggregated snapshot", expectedSnapshot.equals(actualSnapshot));
    // verify cluster wide aggregation on raw data with outdated node stats
    StatsSnapshot.aggregate(expectedSnapshot, outdatedStoreSnapshots.get(0));
    StatsSnapshot rawSnapshot = mapper.readValue(results.getFirst(), StatsSnapshot.class);
    assertTrue("Mismatch in the raw data aggregated snapshot", expectedSnapshot.equals(rawSnapshot));
    // verify aggregator keeps track of instances where exception occurred.
    assertEquals("Mismatch in instances where exception occurred", Collections.singletonList(EXCEPTION_INSTANCE_NAME),
        clusterAggregator.getExceptionOccurredInstances(StatsReportType.ACCOUNT_REPORT));
  }

  /**
   * Tests to verify cluster wide aggregation with outdated node stats for partition class stats report.
   * @throws IOException
   */
  @Test
  public void testDoWorkWithOutdatedNodeForPartitionClass() throws IOException {
    long seed = 1111;
    List<StatsSnapshot> upToDateStoreSnapshots = new ArrayList<>();
    List<StatsSnapshot> outdatedStoreSnapshots = new ArrayList<>();
    upToDateStoreSnapshots.add(generateStoreStats(5, 3, new Random(seed), StatsReportType.PARTITION_CLASS_REPORT));
    outdatedStoreSnapshots.add(generateStoreStats(6, 3, new Random(seed), StatsReportType.PARTITION_CLASS_REPORT));
    StatsWrapper upToDateNodeStats =
        generateNodeStats(upToDateStoreSnapshots, TimeUnit.MINUTES.toMillis(2 * RELEVANT_PERIOD_IN_MINUTES),
            StatsReportType.PARTITION_CLASS_REPORT);
    StatsWrapper outdatedNodeStats =
        generateNodeStats(outdatedStoreSnapshots, 0, StatsReportType.PARTITION_CLASS_REPORT);
    StatsWrapper emptyNodeStats =
        generateNodeStats(Collections.EMPTY_LIST, TimeUnit.MINUTES.toMillis(2 * RELEVANT_PERIOD_IN_MINUTES),
            StatsReportType.PARTITION_CLASS_REPORT);
    Map<String, String> instanceStatsMap = new LinkedHashMap<>();
    instanceStatsMap.put("Instance_0", mapper.writeValueAsString(outdatedNodeStats));
    instanceStatsMap.put("Instance_1", mapper.writeValueAsString(upToDateNodeStats));
    instanceStatsMap.put("Instance_2", mapper.writeValueAsString(emptyNodeStats));
    instanceStatsMap.put(EXCEPTION_INSTANCE_NAME, "");
    Pair<String, String> results = clusterAggregator.doWork(instanceStatsMap, StatsReportType.PARTITION_CLASS_REPORT);

    // verify cluster wide aggregation on raw data with outdated node stats
    StatsSnapshot expectedRawSnapshot = new StatsSnapshot(0L, new HashMap<>());

    StatsSnapshot.aggregate(expectedRawSnapshot, outdatedNodeStats.getSnapshot());
    StatsSnapshot.aggregate(expectedRawSnapshot, upToDateNodeStats.getSnapshot());
    expectedRawSnapshot = HelixClusterAggregator.reduceByPartitionClass(expectedRawSnapshot);
    StatsSnapshot rawSnapshot = mapper.readValue(results.getFirst(), StatsSnapshot.class);
    assertTrue("Mismatch in the raw data aggregated snapshot", expectedRawSnapshot.equals(rawSnapshot));

    StatsSnapshot expectedSnapshot = HelixClusterAggregator.reduceByPartitionClass(upToDateNodeStats.getSnapshot());

    // verify cluster wide aggregation on valid data with outdated node stats
    StatsSnapshot actualSnapshot = mapper.readValue(results.getSecond(), StatsSnapshot.class);
    assertTrue("Mismatch in the valid data aggregated snapshot", expectedSnapshot.equals(actualSnapshot));
    // verify aggregator keeps track of instances where exception occurred.
    assertEquals("Mismatch in instances where exception occurred", Collections.singletonList(EXCEPTION_INSTANCE_NAME),
        clusterAggregator.getExceptionOccurredInstances(StatsReportType.PARTITION_CLASS_REPORT));
  }

  /**
   * Tests to verify cluster wide aggregation at account level with node stats that contain different partition stats.
   * @throws IOException
   */
  @Test
  public void testDoWorkWithDiffNodeStatsForAccount() throws IOException {
    long seed = 1234;
    List<StatsSnapshot> greaterStoreSnapshots = new ArrayList<>();
    List<StatsSnapshot> smallerStoreSnapshots = new ArrayList<>();
    greaterStoreSnapshots.add(generateStoreStats(6, 3, new Random(seed), StatsReportType.ACCOUNT_REPORT));
    smallerStoreSnapshots.add(generateStoreStats(5, 3, new Random(seed), StatsReportType.ACCOUNT_REPORT));
    StatsWrapper greaterNodeStats =
        generateNodeStats(greaterStoreSnapshots, DEFAULT_TIMESTAMP, StatsReportType.ACCOUNT_REPORT);
    StatsWrapper smallerNodeStats =
        generateNodeStats(smallerStoreSnapshots, DEFAULT_TIMESTAMP, StatsReportType.ACCOUNT_REPORT);
    StatsWrapper emptyNodeStats =
        generateNodeStats(Collections.EMPTY_LIST, DEFAULT_TIMESTAMP, StatsReportType.ACCOUNT_REPORT);
    Map<String, String> instanceStatsMap = new LinkedHashMap<>();
    instanceStatsMap.put("Instance_0", mapper.writeValueAsString(smallerNodeStats));
    instanceStatsMap.put("Instance_1", mapper.writeValueAsString(greaterNodeStats));
    instanceStatsMap.put("Instance_2", mapper.writeValueAsString(emptyNodeStats));
    instanceStatsMap.put(EXCEPTION_INSTANCE_NAME, "");
    Pair<String, String> results = clusterAggregator.doWork(instanceStatsMap, StatsReportType.ACCOUNT_REPORT);
    StatsSnapshot expectedSnapshot = greaterStoreSnapshots.get(0);

    // verify cluster wide aggregation on valid data with different node stats
    StatsSnapshot actualSnapshot = mapper.readValue(results.getSecond(), StatsSnapshot.class);
    assertTrue("Mismatch in the valid data aggregated snapshot", expectedSnapshot.equals(actualSnapshot));
    // verify cluster wide aggregation on raw data with different node stats
    StatsSnapshot.aggregate(expectedSnapshot, smallerStoreSnapshots.get(0));
    StatsSnapshot rawSnapshot = mapper.readValue(results.getFirst(), StatsSnapshot.class);
    assertTrue("Mismatch in the raw data aggregated snapshot", expectedSnapshot.equals(rawSnapshot));
    // verify aggregator keeps track of instances where exception occurred.
    assertEquals("Mismatch in instances where exception occurred", Collections.singletonList(EXCEPTION_INSTANCE_NAME),
        clusterAggregator.getExceptionOccurredInstances(StatsReportType.ACCOUNT_REPORT));
  }

  /**
   * Tests to verify cluster wide aggregation at partition class level with node stats that contain different partition stats.
   * @throws IOException
   */
  @Test
  public void testDoWorkWithDiffNodeStatsForPartitionClass() throws IOException {
    long seed = 1234;
    List<StatsSnapshot> greaterStoreSnapshots = new ArrayList<>();
    List<StatsSnapshot> mediumStoreSnapshots = new ArrayList<>();
    List<StatsSnapshot> smallerStoreSnapshots = new ArrayList<>();
    greaterStoreSnapshots.add(generateStoreStats(6, 3, new Random(seed), StatsReportType.PARTITION_CLASS_REPORT));
    mediumStoreSnapshots.add(generateStoreStats(5, 3, new Random(seed), StatsReportType.PARTITION_CLASS_REPORT));
    smallerStoreSnapshots.add(generateStoreStats(4, 3, new Random(seed), StatsReportType.PARTITION_CLASS_REPORT));
    StatsWrapper greaterNodeStats =
        generateNodeStats(greaterStoreSnapshots, DEFAULT_TIMESTAMP, StatsReportType.PARTITION_CLASS_REPORT);
    StatsWrapper mediumNodeStats =
        generateNodeStats(mediumStoreSnapshots, DEFAULT_TIMESTAMP, StatsReportType.PARTITION_CLASS_REPORT);
    StatsWrapper smallerNodeStats =
        generateNodeStats(smallerStoreSnapshots, DEFAULT_TIMESTAMP, StatsReportType.PARTITION_CLASS_REPORT);
    StatsWrapper emptyNodeStats =
        generateNodeStats(Collections.EMPTY_LIST, DEFAULT_TIMESTAMP, StatsReportType.PARTITION_CLASS_REPORT);

    Map<String, String> instanceStatsMap = new LinkedHashMap<>();
    // testing order is (1)smaller, (2)greater, (3)medium (4)empty (5)invalid to ensure the aggregation can correctly choose the replica with largest value
    instanceStatsMap.put("Instance_0", mapper.writeValueAsString(smallerNodeStats));
    instanceStatsMap.put("Instance_1", mapper.writeValueAsString(greaterNodeStats));
    instanceStatsMap.put("Instance_2", mapper.writeValueAsString(mediumNodeStats));
    instanceStatsMap.put("Instance_3", mapper.writeValueAsString(emptyNodeStats));
    instanceStatsMap.put(EXCEPTION_INSTANCE_NAME, "");
    Pair<String, String> partitionClassResults =
        clusterAggregator.doWork(instanceStatsMap, StatsReportType.PARTITION_CLASS_REPORT);

    // verify cluster wide aggregation on raw data with different node stats
    StatsSnapshot expectedRawSnapshot = new StatsSnapshot(0L, new HashMap<>());
    StatsSnapshot.aggregate(expectedRawSnapshot, smallerNodeStats.getSnapshot());
    StatsSnapshot.aggregate(expectedRawSnapshot, mediumNodeStats.getSnapshot());
    StatsSnapshot.aggregate(expectedRawSnapshot, greaterNodeStats.getSnapshot());

    StatsSnapshot rawSnapshot = mapper.readValue(partitionClassResults.getFirst(), StatsSnapshot.class);
    expectedRawSnapshot = HelixClusterAggregator.reduceByPartitionClass(expectedRawSnapshot);
    assertTrue("Mismatch in the raw data aggregated snapshot", expectedRawSnapshot.equals(rawSnapshot));

    // verify cluster wide aggregation on valid data with different node stats
    StatsSnapshot validSnapshot = mapper.readValue(partitionClassResults.getSecond(), StatsSnapshot.class);
    StatsSnapshot expectedValidSnapshot = HelixClusterAggregator.reduceByPartitionClass(greaterNodeStats.getSnapshot());
    assertTrue("Mismatch in the valid data aggregated snapshot", expectedValidSnapshot.equals(validSnapshot));
    // verify aggregator keeps track of instances where exception occurred.
    assertEquals("Mismatch in instances where exception occurred", Collections.singletonList(EXCEPTION_INSTANCE_NAME),
        clusterAggregator.getExceptionOccurredInstances(StatsReportType.PARTITION_CLASS_REPORT));
  }

  /**
   * Given a {@link List} of {@link StatsSnapshot}s, a timestamp and specified {@link StatsReportType}, the method
   * will generate a {@link StatsWrapper} that would have been produced by a node
   * @param storeSnapshots a {@link List} of store level {@link StatsSnapshot}s
   * @param timestamp the timestamp to be attached to the generated {@link StatsWrapper}
   * @param type the type of stats report which is enum defined in {@link StatsReportType}
   * @return the generated node level {@link StatsWrapper}
   */
  private StatsWrapper generateNodeStats(List<StatsSnapshot> storeSnapshots, long timestamp, StatsReportType type) {
    Map<String, StatsSnapshot> partitionMap = new HashMap<>();
    Map<String, StatsSnapshot> partitionClassMap = new HashMap<>();
    String[] PARTITION_CLASS = new String[]{"PartitionClass1", "PartitionClass2"};
    long total = 0;
    int numbOfPartitions = storeSnapshots.size();
    for (int i = 0; i < numbOfPartitions; i++) {
      String PartitionIdStr = "Partition[" + i + "]";
      StatsSnapshot storeSnapshot = storeSnapshots.get(i);
      partitionMap.put(PartitionIdStr, storeSnapshot);
      total += storeSnapshot.getValue();
      if (type == StatsReportType.PARTITION_CLASS_REPORT) {
        String partitionClassStr = PARTITION_CLASS[i % PARTITION_CLASS.length];
        StatsSnapshot partitionClassSnapshot =
            partitionClassMap.getOrDefault(partitionClassStr, new StatsSnapshot(0L, new HashMap<>()));
        partitionClassSnapshot.setValue(partitionClassSnapshot.getValue() + storeSnapshot.getValue());
        partitionClassSnapshot.getSubMap().put(PartitionIdStr, storeSnapshot);
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
        new StatsHeader(StatsHeader.StatsDescription.QUOTA, timestamp, numbOfPartitions, numbOfPartitions,
            Collections.emptyList());
    return new StatsWrapper(header, nodeSnapshot);
  }

  /**
   * Generate a specific quota {@link StatsSnapshot} based on the given parameters that would have been produced by a
   * {@link com.github.ambry.store.Store}.
   * @param accountCount number of account entry in the {@link StatsSnapshot}
   * @param containerCount number of container entry in the {@link StatsSnapshot}
   * @param random the random generator to be used
   * @param type the type of stats report which is enum defined in {@link StatsReportType}
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
}
