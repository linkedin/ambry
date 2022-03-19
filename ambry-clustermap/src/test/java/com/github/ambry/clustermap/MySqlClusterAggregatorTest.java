/**
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.server.HostAccountStorageStatsWrapper;
import com.github.ambry.server.HostPartitionClassStorageStatsWrapper;
import com.github.ambry.server.StatsHeader;
import com.github.ambry.server.StorageStatsUtilTest;
import com.github.ambry.server.storagestats.AggregatedAccountStorageStats;
import com.github.ambry.server.storagestats.AggregatedPartitionClassStorageStats;
import com.github.ambry.server.storagestats.ContainerStorageStats;
import com.github.ambry.server.storagestats.HostAccountStorageStats;
import com.github.ambry.server.storagestats.HostPartitionClassStorageStats;
import com.github.ambry.utils.Pair;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;


/**
 * Unit test for {@link MySqlClusterAggregator}.
 */
public class MySqlClusterAggregatorTest {
  private static final long RELEVANT_PERIOD_IN_MINUTES = 60;
  private static final long DEFAULT_TIMESTAMP = 1000;
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private final MySqlClusterAggregator clusterAggregator;

  public MySqlClusterAggregatorTest() {
    clusterAggregator = new MySqlClusterAggregator(RELEVANT_PERIOD_IN_MINUTES);
  }

  /**
   * Test for {@link MySqlClusterAggregator#sumPhysicalStorageUsage}.
   */
  @Test
  public void testSumPhysicalStorageUsage() {
    Map<Short, Map<Short, ContainerStorageStats>> testcase = new HashMap<Short, Map<Short, ContainerStorageStats>>() {
      {
        put((short) 1, new HashMap<Short, ContainerStorageStats>() {
          {
            put((short) 1, new ContainerStorageStats((short) 1, 0, 1, 1));
            put((short) 2, new ContainerStorageStats((short) 2, 0, 2, 1));
          }
        });
        put((short) 2, new HashMap<Short, ContainerStorageStats>() {
          {
            put((short) 3, new ContainerStorageStats((short) 3, 0, 3, 1));
            put((short) 4, new ContainerStorageStats((short) 4, 0, 4, 1));
            put((short) 5, new ContainerStorageStats((short) 5, 0, 5, 1));
          }
        });
      }
    };
    // Summing the physical storage usages of all the container storage stats in the map up.
    long sum = clusterAggregator.sumPhysicalStorageUsage(testcase);
    long expected = 1 + 2 + 3 + 4 + 5;
    Assert.assertEquals(expected, sum);
  }

  /**
   * Test for {@link MySqlClusterAggregator#mergeAccountStorageStatsMap}.
   */
  @Test
  public void testMergeAccountStorageStatsMap() {
    Map<Short, Map<Short, ContainerStorageStats>> testcase1 = new HashMap<Short, Map<Short, ContainerStorageStats>>() {
      {
        put((short) 1, new HashMap<Short, ContainerStorageStats>() {
          {
            put((short) 1, new ContainerStorageStats((short) 1, 10, 20, 1));
            put((short) 2, new ContainerStorageStats((short) 2, 20, 40, 1));
          }
        });
        put((short) 2, new HashMap<Short, ContainerStorageStats>() {
          {
            put((short) 3, new ContainerStorageStats((short) 3, 30, 60, 1));
            put((short) 4, new ContainerStorageStats((short) 4, 40, 80, 1));
            put((short) 5, new ContainerStorageStats((short) 5, 50, 100, 1));
          }
        });
      }
    };

    Map<Short, Map<Short, ContainerStorageStats>> result = new HashMap<>();
    // Merge the testcase1 to an empty map, the result would be the same as testcase1.
    clusterAggregator.mergeAccountStorageStatsMap(result, testcase1);
    Assert.assertEquals(testcase1, result);

    Map<Short, Map<Short, ContainerStorageStats>> testcase2 = new HashMap<Short, Map<Short, ContainerStorageStats>>() {
      {
        put((short) 2, new HashMap<Short, ContainerStorageStats>() {
          {
            put((short) 4, new ContainerStorageStats((short) 4, 40, 80, 1));
            put((short) 5, new ContainerStorageStats((short) 5, 50, 100, 1));
            put((short) 6, new ContainerStorageStats((short) 6, 60, 120, 1));
            put((short) 7, new ContainerStorageStats((short) 7, 70, 140, 1));
          }
        });
        put((short) 3, new HashMap<Short, ContainerStorageStats>() {
          {
            put((short) 8, new ContainerStorageStats((short) 8, 80, 160, 1));
          }
        });
      }
    };

    Map<Short, Map<Short, ContainerStorageStats>> expected = new HashMap<Short, Map<Short, ContainerStorageStats>>() {
      {
        put((short) 1, new HashMap<Short, ContainerStorageStats>() {
          {
            put((short) 1, new ContainerStorageStats((short) 1, 10, 20, 1));
            put((short) 2, new ContainerStorageStats((short) 2, 20, 40, 1));
          }
        });
        put((short) 2, new HashMap<Short, ContainerStorageStats>() {
          {
            put((short) 3, new ContainerStorageStats((short) 3, 30, 60, 1));
            put((short) 4, new ContainerStorageStats((short) 4, 80, 160, 2));
            put((short) 5, new ContainerStorageStats((short) 5, 100, 200, 2));
            put((short) 6, new ContainerStorageStats((short) 6, 60, 120, 1));
            put((short) 7, new ContainerStorageStats((short) 7, 70, 140, 1));
          }
        });
        put((short) 3, new HashMap<Short, ContainerStorageStats>() {
          {
            put((short) 8, new ContainerStorageStats((short) 8, 80, 160, 1));
          }
        });
      }
    };

    // Merge testcase1 with testcase2
    clusterAggregator.mergeAccountStorageStatsMap(result, testcase2);
    Assert.assertEquals(expected, result);
  }

  /**
   * Test basic functionality of {@link MySqlClusterAggregator#aggregateHostAccountStorageStats}.
   * @throws Exception
   */
  @Test
  public void testAggregateHostAccountStorageStats() throws Exception {
    int nodeCount = 3;
    // Partition id to account id to container id to container storage stats.
    Map<Long, Map<Short, Map<Short, ContainerStorageStats>>> storageStatsMap = new HashMap<>();
    // Create storage stats for 3 partitions, each has 3, 4 and 5 accounts.
    for (int i = 0; i < 3; i++) {
      storageStatsMap.put((long) i,
          StorageStatsUtilTest.generateRandomAggregatedAccountStorageStats((short) 0, i + 3, 3, 10000L, 2, 10));
    }
    StatsHeader header = new StatsHeader(StatsHeader.StatsDescription.STORED_DATA_SIZE, DEFAULT_TIMESTAMP, 3, 3,
        Collections.emptyList());
    HostAccountStorageStatsWrapper nodeStats =
        new HostAccountStorageStatsWrapper(header, new HostAccountStorageStats(storageStatsMap));

    header = new StatsHeader(StatsHeader.StatsDescription.STORED_DATA_SIZE, DEFAULT_TIMESTAMP, 0, 0,
        Collections.emptyList());
    HostAccountStorageStatsWrapper emptyStats =
        new HostAccountStorageStatsWrapper(header, new HostAccountStorageStats());

    Map<String, HostAccountStorageStatsWrapper> instanceToStatsMap = new HashMap<>();

    // For each node, use the same HostAccountStorageStats data.
    // So the combined raw data would be three times of the aggregated data (we have 3 nodes).
    for (int i = 0; i < nodeCount; i++) {
      instanceToStatsMap.put("Instance_" + i, new HostAccountStorageStatsWrapper(new StatsHeader(nodeStats.getHeader()),
          new HostAccountStorageStats(nodeStats.getStats())));
    }
    instanceToStatsMap.put("Instance_" + nodeCount, emptyStats);

    Pair<AggregatedAccountStorageStats, AggregatedAccountStorageStats> aggregatedRawAndValidStats =
        clusterAggregator.aggregateHostAccountStorageStatsWrappers(instanceToStatsMap);
    Map<Short, Map<Short, ContainerStorageStats>> expectedAggregatedStorageStatsMap =
        clusterAggregator.aggregateHostAccountStorageStats(storageStatsMap);

    Assert.assertEquals(expectedAggregatedStorageStatsMap, aggregatedRawAndValidStats.getSecond().getStorageStats());
    assertAggregatedRawStatsForAccountStorageStats(aggregatedRawAndValidStats.getFirst().getStorageStats(),
        expectedAggregatedStorageStatsMap, nodeCount);
  }

  /**
   * Test basic functionality of {@link MySqlClusterAggregator#aggregateHostPartitionClassStorageStats}.
   * @throws Exception
   */
  @Test
  public void testAggregateHostPartitionClassStorageStats() throws Exception {
    int nodeCount = 3;
    int numberOfPartitions = 4;
    Map<String, Map<Long, Map<Short, Map<Short, ContainerStorageStats>>>> storageStatsMap = new HashMap<>();
    String[] partitionClassNames = {"default", "newClass"};
    for (int i = 0; i < numberOfPartitions; i++) {
      String partitionClassName = partitionClassNames[i % partitionClassNames.length];
      storageStatsMap.computeIfAbsent(partitionClassName, k -> new HashMap<>())
          .put((long) i,
              StorageStatsUtilTest.generateRandomAggregatedAccountStorageStats((short) 0, i + 3, 3, 10000L, 2, 10));
    }
    StatsHeader header =
        new StatsHeader(StatsHeader.StatsDescription.STORED_DATA_SIZE, DEFAULT_TIMESTAMP, numberOfPartitions,
            numberOfPartitions, Collections.emptyList());
    HostPartitionClassStorageStatsWrapper nodeStats =
        new HostPartitionClassStorageStatsWrapper(header, new HostPartitionClassStorageStats(storageStatsMap));

    header = new StatsHeader(StatsHeader.StatsDescription.STORED_DATA_SIZE, DEFAULT_TIMESTAMP, 0, 0,
        Collections.emptyList());
    HostPartitionClassStorageStatsWrapper emptyStats =
        new HostPartitionClassStorageStatsWrapper(header, new HostPartitionClassStorageStats());

    Map<String, HostPartitionClassStorageStatsWrapper> instanceToStatsMap = new HashMap<>();

    for (int i = 0; i < nodeCount; i++) {
      instanceToStatsMap.put("Instance_" + i,
          new HostPartitionClassStorageStatsWrapper(new StatsHeader(nodeStats.getHeader()),
              new HostPartitionClassStorageStats(nodeStats.getStats())));
    }
    instanceToStatsMap.put("Instance_" + nodeCount, emptyStats);

    Pair<AggregatedPartitionClassStorageStats, AggregatedPartitionClassStorageStats> aggregatedRawAndValidStats =
        clusterAggregator.aggregateHostPartitionClassStorageStatsWrappers(instanceToStatsMap);
    Map<String, Map<Short, Map<Short, ContainerStorageStats>>> expectedAggregatedStorageStatsMap =
        clusterAggregator.aggregateHostPartitionClassStorageStats(storageStatsMap);

    Assert.assertEquals(expectedAggregatedStorageStatsMap, aggregatedRawAndValidStats.getSecond().getStorageStats());
    assertAggregatedRawStatsForPartitionClassStorageStats(aggregatedRawAndValidStats.getFirst().getStorageStats(),
        expectedAggregatedStorageStatsMap, nodeCount);
  }

  /**
   * Test {@link MySqlClusterAggregator#aggregateHostAccountStorageStatsWrappers}, but with different numbers of partitions
   * from different nodes.
   * @throws Exception
   */
  @Test
  public void testAggregateHostAccountStorageStatsWithDifferentNumberOfStores() throws Exception {
    // This storage stats has only 2 partitions, partition 0 and partition 1. Each partition has only one account and one container.
    String statsInJson =
        "{'0': {'0': {'0': {'containerId':0, 'logicalStorageUsage':10, 'physicalStorageUsage':20, 'numberOfBlobs':10}}}, '1': {'0': {'0': {'containerId':0, 'logicalStorageUsage':20, 'physicalStorageUsage':40, 'numberOfBlobs':20}}}}";
    Map<Long, Map<Short, Map<Short, ContainerStorageStats>>> storageStatsMap1 =
        objectMapper.readValue(statsInJson.replace("'", "\""), HostAccountStorageStats.class).getStorageStats();
    // This storage stats has only 3 partitions, partition 0, partition 1 and partition 2. Each partition has only one account and one container.
    statsInJson =
        "{'0': {'0': {'0': {'containerId':0, 'logicalStorageUsage':30, 'physicalStorageUsage':60, 'numberOfBlobs':30}}}, '1': {'0': {'0': {'containerId':0, 'logicalStorageUsage':40, 'physicalStorageUsage':80, 'numberOfBlobs':40}}}, '2': {'0': {'0': {'containerId':0, 'logicalStorageUsage':50, 'physicalStorageUsage':100, 'numberOfBlobs':50}}}}";
    Map<Long, Map<Short, Map<Short, ContainerStorageStats>>> storageStatsMap2 =
        objectMapper.readValue(statsInJson.replace("'", "\""), HostAccountStorageStats.class).getStorageStats();
    // Raw combined storage stats should only have one account and one container, and its storage stats is the sum of storage stats from all nodes.
    statsInJson =
        "{'0': {'0': {'containerId':0, 'logicalStorageUsage':150, 'physicalStorageUsage':300, 'numberOfBlobs':150}}}";
    Map<Short, Map<Short, ContainerStorageStats>> expectedRaw = objectMapper.readValue(statsInJson.replace("'", "\""),
        new TypeReference<Map<Short, Map<Short, ContainerStorageStats>>>() {
        });
    // Aggregated storage stats should only have one account and one container, and its storage stats is the sum of storage stats from second node,
    // since second node's physical storage usage is larger than first node at every partition.
    statsInJson =
        "{'0': {'0': {'containerId':0, 'logicalStorageUsage':120, 'physicalStorageUsage':240, 'numberOfBlobs':120}}}";
    Map<Short, Map<Short, ContainerStorageStats>> expectedValid = objectMapper.readValue(statsInJson.replace("'", "\""),
        new TypeReference<Map<Short, Map<Short, ContainerStorageStats>>>() {
        });
    // StorageStatsMap1 only have 2 store stats,  StorageStatsMap2 has 3
    StatsHeader header = new StatsHeader(StatsHeader.StatsDescription.STORED_DATA_SIZE, DEFAULT_TIMESTAMP, 2, 2,
        Collections.emptyList());
    HostAccountStorageStatsWrapper nodeStats1 =
        new HostAccountStorageStatsWrapper(header, new HostAccountStorageStats(storageStatsMap1));

    header = new StatsHeader(StatsHeader.StatsDescription.STORED_DATA_SIZE, DEFAULT_TIMESTAMP, 3, 3,
        Collections.emptyList());
    HostAccountStorageStatsWrapper nodeStats2 =
        new HostAccountStorageStatsWrapper(header, new HostAccountStorageStats(storageStatsMap2));

    Map<String, HostAccountStorageStatsWrapper> instanceStatsMap = new HashMap<>();
    instanceStatsMap.put("Instance_1", nodeStats1);
    instanceStatsMap.put("Instance_2", nodeStats2);

    Pair<AggregatedAccountStorageStats, AggregatedAccountStorageStats> aggregatedRawAndValidStats =
        clusterAggregator.aggregateHostAccountStorageStatsWrappers(instanceStatsMap);
    Assert.assertEquals(expectedRaw, aggregatedRawAndValidStats.getFirst().getStorageStats());
    Assert.assertEquals(expectedValid, aggregatedRawAndValidStats.getSecond().getStorageStats());
  }

  /**
   * Test {@link MySqlClusterAggregator#aggregateHostAccountStorageStatsWrappers} but with one node having outdated
   * storage stats data. Outdated data shouldn't be used when aggregating valid storage usage.
   * @throws Exception
   */
  @Test
  public void testAggregateHostAccountStorageStatsWithOutdatedNode() throws Exception {
    Map<Long, Map<Short, Map<Short, ContainerStorageStats>>> upToDateStorageStatsMap = new HashMap<>();
    upToDateStorageStatsMap.put((long) 0,
        StorageStatsUtilTest.generateRandomAggregatedAccountStorageStats((short) 0, 5, 3, 10000L, 2, 10));
    Map<Long, Map<Short, Map<Short, ContainerStorageStats>>> outdatedStorageStatsMap = new HashMap<>();
    outdatedStorageStatsMap.put((long) 0,
        StorageStatsUtilTest.generateRandomAggregatedAccountStorageStats((short) 0, 6, 3, 10000L, 2, 10));

    StatsHeader header = new StatsHeader(StatsHeader.StatsDescription.STORED_DATA_SIZE,
        TimeUnit.MINUTES.toMillis(2 * RELEVANT_PERIOD_IN_MINUTES), 1, 1, Collections.emptyList());
    HostAccountStorageStatsWrapper upToDateNodeStats =
        new HostAccountStorageStatsWrapper(header, new HostAccountStorageStats(upToDateStorageStatsMap));
    header = new StatsHeader(StatsHeader.StatsDescription.STORED_DATA_SIZE, 0, 1, 1, Collections.emptyList());
    HostAccountStorageStatsWrapper outdatedNodeStats =
        new HostAccountStorageStatsWrapper(header, new HostAccountStorageStats(outdatedStorageStatsMap));
    header = new StatsHeader(StatsHeader.StatsDescription.STORED_DATA_SIZE,
        TimeUnit.MINUTES.toMillis(2 * RELEVANT_PERIOD_IN_MINUTES), 0, 0, Collections.emptyList());
    HostAccountStorageStatsWrapper emptyNodeStats =
        new HostAccountStorageStatsWrapper(header, new HostAccountStorageStats());

    Map<String, HostAccountStorageStatsWrapper> instanceToStatsMap = new LinkedHashMap<>();
    instanceToStatsMap.put("Instance_0", upToDateNodeStats);
    instanceToStatsMap.put("Instance_1", outdatedNodeStats);
    instanceToStatsMap.put("Instance_2", emptyNodeStats);

    Pair<AggregatedAccountStorageStats, AggregatedAccountStorageStats> aggregatedRawAndValidStats =
        clusterAggregator.aggregateHostAccountStorageStatsWrappers(instanceToStatsMap);

    Map<Short, Map<Short, ContainerStorageStats>> expectedValid =
        clusterAggregator.aggregateHostAccountStorageStats(upToDateStorageStatsMap);
    Assert.assertEquals(expectedValid, aggregatedRawAndValidStats.getSecond().getStorageStats());
  }

  /**
   * Test {@link MySqlClusterAggregator#aggregateHostPartitionClassStorageStatsWrappers} but with one node having outdated
   * storage stats data. Outdated data shouldn't be used when aggregating valid storage stats.
   * @throws Exception
   */
  @Test
  public void testAggregateHostPartitionClassStorageStatsWithOutdatedNode() throws Exception {
    Map<String, Map<Long, Map<Short, Map<Short, ContainerStorageStats>>>> upToDateStorageStatsMap = new HashMap<>();
    upToDateStorageStatsMap.computeIfAbsent("default", k -> new HashMap<>())
        .put((long) 0,
            StorageStatsUtilTest.generateRandomAggregatedAccountStorageStats((short) 0, 5, 3, 10000L, 2, 10));
    Map<String, Map<Long, Map<Short, Map<Short, ContainerStorageStats>>>> outdatedStorageStatsMap = new HashMap<>();
    outdatedStorageStatsMap.computeIfAbsent("default", k -> new HashMap<>())
        .put((long) 0,
            StorageStatsUtilTest.generateRandomAggregatedAccountStorageStats((short) 0, 5, 3, 10000L, 2, 10));

    StatsHeader header = new StatsHeader(StatsHeader.StatsDescription.STORED_DATA_SIZE,
        TimeUnit.MINUTES.toMillis(2 * RELEVANT_PERIOD_IN_MINUTES), 1, 1, Collections.emptyList());
    HostPartitionClassStorageStatsWrapper upToDateNodeStats =
        new HostPartitionClassStorageStatsWrapper(header, new HostPartitionClassStorageStats(upToDateStorageStatsMap));
    header = new StatsHeader(StatsHeader.StatsDescription.STORED_DATA_SIZE, 0, 1, 1, Collections.emptyList());
    HostPartitionClassStorageStatsWrapper outdatedNodeStats =
        new HostPartitionClassStorageStatsWrapper(header, new HostPartitionClassStorageStats(outdatedStorageStatsMap));
    header = new StatsHeader(StatsHeader.StatsDescription.STORED_DATA_SIZE,
        TimeUnit.MINUTES.toMillis(2 * RELEVANT_PERIOD_IN_MINUTES), 0, 0, Collections.emptyList());
    HostPartitionClassStorageStatsWrapper emptyNodeStats =
        new HostPartitionClassStorageStatsWrapper(header, new HostPartitionClassStorageStats());

    Map<String, HostPartitionClassStorageStatsWrapper> instanceToStatsMap = new LinkedHashMap<>();
    instanceToStatsMap.put("Instance_0", upToDateNodeStats);
    instanceToStatsMap.put("Instance_1", outdatedNodeStats);
    instanceToStatsMap.put("Instance_2", emptyNodeStats);

    Pair<AggregatedPartitionClassStorageStats, AggregatedPartitionClassStorageStats> aggregatedRawAndValidStats =
        clusterAggregator.aggregateHostPartitionClassStorageStatsWrappers(instanceToStatsMap);

    Map<String, Map<Short, Map<Short, ContainerStorageStats>>> expectedValid =
        clusterAggregator.aggregateHostPartitionClassStorageStats(upToDateStorageStatsMap);
    Assert.assertEquals(expectedValid, aggregatedRawAndValidStats.getSecond().getStorageStats());
  }

  /**
   * Test {@link MySqlClusterAggregator#aggregateHostAccountStorageStatsWrappers} with one node having earlier timestamp
   * but larger storage stats data. Larger data should be used when aggregating valid storage stats.
   * @throws Exception
   */
  @Test
  public void testAggregateHostAccountStorageStatsWithEarlyNode() throws Exception {
    Map<Long, Map<Short, Map<Short, ContainerStorageStats>>> upToDateStorageStatsMap = new HashMap<>();
    upToDateStorageStatsMap.put((long) 0,
        StorageStatsUtilTest.generateRandomAggregatedAccountStorageStats((short) 0, 5, 3, 10L, 2, 10));
    // Use a much larger maximum number when creating early storage stats so the values would be larger here.
    Map<Long, Map<Short, Map<Short, ContainerStorageStats>>> earlyStorageStatsMap = new HashMap<>();
    earlyStorageStatsMap.put((long) 0,
        StorageStatsUtilTest.generateRandomAggregatedAccountStorageStats((short) 0, 6, 3, 10000L, 2, 10));

    StatsHeader header = new StatsHeader(StatsHeader.StatsDescription.STORED_DATA_SIZE,
        TimeUnit.MINUTES.toMillis(RELEVANT_PERIOD_IN_MINUTES / 2), 1, 1, Collections.emptyList());
    HostAccountStorageStatsWrapper upToDateNodeStats =
        new HostAccountStorageStatsWrapper(header, new HostAccountStorageStats(upToDateStorageStatsMap));
    header = new StatsHeader(StatsHeader.StatsDescription.STORED_DATA_SIZE, 0, 1, 1, Collections.emptyList());
    HostAccountStorageStatsWrapper earlyNodeStats =
        new HostAccountStorageStatsWrapper(header, new HostAccountStorageStats(earlyStorageStatsMap));

    Map<String, HostAccountStorageStatsWrapper> instanceToStatsMap = new LinkedHashMap<>();
    instanceToStatsMap.put("Instance_0", upToDateNodeStats);
    instanceToStatsMap.put("Instance_1", earlyNodeStats);

    Pair<AggregatedAccountStorageStats, AggregatedAccountStorageStats> aggregatedRawAndValidStats =
        clusterAggregator.aggregateHostAccountStorageStatsWrappers(instanceToStatsMap);

    Map<Short, Map<Short, ContainerStorageStats>> expectedValid =
        clusterAggregator.aggregateHostAccountStorageStats(earlyStorageStatsMap);
    Assert.assertEquals(expectedValid, aggregatedRawAndValidStats.getSecond().getStorageStats());
  }

  private void assertAggregatedRawStatsForAccountStorageStats(Map<Short, Map<Short, ContainerStorageStats>> raw,
      Map<Short, Map<Short, ContainerStorageStats>> expected, int nodeCount) {
    Assert.assertEquals(expected.size(), raw.size());
    for (Map.Entry<Short, Map<Short, ContainerStorageStats>> rawEntry : raw.entrySet()) {
      short accountId = rawEntry.getKey();
      Assert.assertTrue(expected.containsKey(accountId));
      Assert.assertEquals(expected.get(accountId).size(), rawEntry.getValue().size());
      for (Map.Entry<Short, ContainerStorageStats> rawContainerEntry : rawEntry.getValue().entrySet()) {
        short containerId = rawContainerEntry.getKey();
        Assert.assertTrue(expected.get(accountId).containsKey(containerId));
        ContainerStorageStats expectedStats = expected.get(accountId).get(containerId);
        ContainerStorageStats rawStats = rawContainerEntry.getValue();
        Assert.assertEquals(expectedStats.getLogicalStorageUsage() * nodeCount, rawStats.getLogicalStorageUsage());
        Assert.assertEquals(expectedStats.getPhysicalStorageUsage() * nodeCount, rawStats.getPhysicalStorageUsage());
        Assert.assertEquals(expectedStats.getNumberOfBlobs() * nodeCount, rawStats.getNumberOfBlobs());
      }
    }
  }

  private void assertAggregatedRawStatsForPartitionClassStorageStats(
      Map<String, Map<Short, Map<Short, ContainerStorageStats>>> raw,
      Map<String, Map<Short, Map<Short, ContainerStorageStats>>> expected, int nodeCount) {
    Assert.assertEquals(expected.size(), raw.size());
    for (Map.Entry<String, Map<Short, Map<Short, ContainerStorageStats>>> rawEntry : raw.entrySet()) {
      String partitionClassName = rawEntry.getKey();
      Assert.assertTrue(expected.containsKey(partitionClassName));
      Assert.assertEquals(expected.get(partitionClassName).size(), rawEntry.getValue().size());
      assertAggregatedRawStatsForAccountStorageStats(rawEntry.getValue(), expected.get(partitionClassName), nodeCount);
    }
  }
}
