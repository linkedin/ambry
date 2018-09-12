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
import com.github.ambry.server.StatsSnapshot;
import com.github.ambry.server.StatsWrapper;
import com.github.ambry.utils.Pair;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
   * Basic tests to verify the cluster wide aggregation.
   * @throws IOException
   */
  @Test
  public void testDoWorkBasic() throws IOException {
    int nodeCount = 3;
    Random random = new Random();
    List<StatsSnapshot> storeSnapshots = new ArrayList<>();
    for (int i = 3; i < 6; i++) {
      storeSnapshots.add(generateStoreStats(i, 3, random));
    }
    StatsWrapper nodeStats = generateNodeStats(storeSnapshots, DEFAULT_TIMESTAMP);
    String nodeStatsJSON = mapper.writeValueAsString(nodeStats);
    StatsWrapper emptyNodeStats = generateNodeStats(Collections.EMPTY_LIST, DEFAULT_TIMESTAMP);
    String emptyNodeStatsJSON = mapper.writeValueAsString(emptyNodeStats);

    Map<String, String> statsWrappersJSON = new HashMap<>();
    for (int i = 0; i < nodeCount; i++) {
      statsWrappersJSON.put("Store_" + i, (nodeStatsJSON));
    }
    statsWrappersJSON.put("Store_" + nodeCount, emptyNodeStatsJSON);
    statsWrappersJSON.put(EXCEPTION_INSTANCE_NAME, "");
    for (int i = 1; i < storeSnapshots.size(); i++) {
      StatsSnapshot.aggregate(storeSnapshots.get(0), storeSnapshots.get(i));
    }
    Pair<String, String> results = clusterAggregator.doWork(statsWrappersJSON);
    StatsSnapshot expectedSnapshot = storeSnapshots.get(0);
    // verify cluster wide raw aggregation
    StatsSnapshot rawSnapshot = mapper.readValue(results.getFirst(), StatsSnapshot.class);
    assertEquals("Mismatch in total value of all accounts", nodeCount * expectedSnapshot.getValue(),
        rawSnapshot.getValue());
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
    // verify cluster wide aggregation
    StatsSnapshot actualSnapshot = mapper.readValue(results.getSecond(), StatsSnapshot.class);
    assertTrue("Mismatch in the aggregated snapshot", expectedSnapshot.equals(actualSnapshot));
    // verify aggregator keeps track of instances where exception occurred.
    assertEquals("Mismatch in instances where exception occurred", Collections.singletonList(EXCEPTION_INSTANCE_NAME),
        clusterAggregator.getExceptionOccurredInstances());
  }

  /**
   * Tests to verify cluster wide aggregation with outdated node stats.
   * @throws IOException
   */
  @Test
  public void testDoWorkWithOutdatedNode() throws IOException {
    long seed = 1111;
    List<StatsSnapshot> upToDateStoreSnapshots = new ArrayList<>();
    List<StatsSnapshot> outdatedStoreSnapshots = new ArrayList<>();
    upToDateStoreSnapshots.add(generateStoreStats(5, 3, new Random(seed)));
    outdatedStoreSnapshots.add(generateStoreStats(6, 3, new Random(seed)));
    StatsWrapper upToDateNodeStats =
        generateNodeStats(upToDateStoreSnapshots, TimeUnit.MINUTES.toMillis(2 * RELEVANT_PERIOD_IN_MINUTES));
    StatsWrapper outdatedNodeStats = generateNodeStats(outdatedStoreSnapshots, 0);
    StatsWrapper emptyNodeStats =
        generateNodeStats(Collections.EMPTY_LIST, TimeUnit.MINUTES.toMillis(2 * RELEVANT_PERIOD_IN_MINUTES));
    Map<String, String> statsWrappersJSON = new HashMap<>();
    statsWrappersJSON.put("Store_0", mapper.writeValueAsString(outdatedNodeStats));
    statsWrappersJSON.put("Store_1", mapper.writeValueAsString(upToDateNodeStats));
    statsWrappersJSON.put("Store_2", mapper.writeValueAsString(emptyNodeStats));
    statsWrappersJSON.put(EXCEPTION_INSTANCE_NAME, "");
    Pair<String, String> results = clusterAggregator.doWork(statsWrappersJSON);
    StatsSnapshot expectedSnapshot = upToDateStoreSnapshots.get(0);
    // verify cluster wide aggregation with outdated node stats
    StatsSnapshot actualSnapshot = mapper.readValue(results.getSecond(), StatsSnapshot.class);
    assertTrue("Mismatch in the aggregated snapshot", expectedSnapshot.equals(actualSnapshot));
    // verify cluster wide raw aggregation with outdated node stats
    StatsSnapshot.aggregate(expectedSnapshot, outdatedStoreSnapshots.get(0));
    StatsSnapshot rawSnapshot = mapper.readValue(results.getFirst(), StatsSnapshot.class);
    assertTrue("Mismatch in the raw aggregated snapshot", expectedSnapshot.equals(rawSnapshot));
    // verify aggregator keeps track of instances where exception occurred.
    assertEquals("Mismatch in instances where exception occurred", Collections.singletonList(EXCEPTION_INSTANCE_NAME),
        clusterAggregator.getExceptionOccurredInstances());
  }

  /**
   * Tests to verify cluster aggregation with node stats that contain different partition stats.
   * @throws IOException
   */
  @Test
  public void testDoWorkWithDiffNodeStats() throws IOException {
    long seed = 1234;
    List<StatsSnapshot> greaterStoreSnapshots = new ArrayList<>();
    List<StatsSnapshot> smallerStoreSnapshots = new ArrayList<>();
    greaterStoreSnapshots.add(generateStoreStats(6, 3, new Random(seed)));
    smallerStoreSnapshots.add(generateStoreStats(5, 3, new Random(seed)));
    StatsWrapper greaterNodeStats = generateNodeStats(greaterStoreSnapshots, DEFAULT_TIMESTAMP);
    StatsWrapper smallerNodeStats = generateNodeStats(smallerStoreSnapshots, DEFAULT_TIMESTAMP);
    StatsWrapper emptyNodeStats = generateNodeStats(Collections.EMPTY_LIST, DEFAULT_TIMESTAMP);
    Map<String, String> statsWrappersJSON = new HashMap<>();
    statsWrappersJSON.put("Store_0", mapper.writeValueAsString(smallerNodeStats));
    statsWrappersJSON.put("Store_1", mapper.writeValueAsString(greaterNodeStats));
    statsWrappersJSON.put("Store_2", mapper.writeValueAsString(emptyNodeStats));
    statsWrappersJSON.put(EXCEPTION_INSTANCE_NAME, "");
    Pair<String, String> results = clusterAggregator.doWork(statsWrappersJSON);
    StatsSnapshot expectedSnapshot = greaterStoreSnapshots.get(0);
    // verify cluster wide aggregation with different node stats
    StatsSnapshot actualSnapshot = mapper.readValue(results.getSecond(), StatsSnapshot.class);
    assertTrue("Mismatch in the aggregated snapshot", expectedSnapshot.equals(actualSnapshot));
    // verify cluster wide raw aggregation with different node stats
    StatsSnapshot.aggregate(expectedSnapshot, smallerStoreSnapshots.get(0));
    StatsSnapshot rawSnapshot = mapper.readValue(results.getFirst(), StatsSnapshot.class);
    assertTrue("Mismatch in the raw aggregated snapshot", expectedSnapshot.equals(rawSnapshot));
    // verify aggregator keeps track of instances where exception occurred.
    assertEquals("Mismatch in instances where exception occurred", Collections.singletonList(EXCEPTION_INSTANCE_NAME),
        clusterAggregator.getExceptionOccurredInstances());
  }

  /**
   * Given a {@link List} of {@link StatsSnapshot}s and a timestamp generate a {@link StatsWrapper} that would have been
   * produced by a node.
   * @param storeSnapshots a {@link List} of store level {@link StatsSnapshot}s.
   * @param timestamp the timestamp to be attached to the generated {@link StatsWrapper}
   * @return the generated node level {@link StatsWrapper}
   */
  private StatsWrapper generateNodeStats(List<StatsSnapshot> storeSnapshots, long timestamp) {
    Map<String, StatsSnapshot> partitionMap = new HashMap<>();
    long total = 0;
    int numbOfPartitions = storeSnapshots.size();
    for (int i = 0; i < numbOfPartitions; i++) {
      StatsSnapshot partitionSnapshot = storeSnapshots.get(i);
      partitionMap.put(String.format("partition_%d", i), partitionSnapshot);
      total += partitionSnapshot.getValue();
    }
    StatsSnapshot nodeSnapshot = new StatsSnapshot(total, partitionMap);
    StatsHeader header =
        new StatsHeader(StatsHeader.StatsDescription.QUOTA, timestamp, numbOfPartitions, numbOfPartitions,
            Collections.EMPTY_LIST);
    return new StatsWrapper(header, nodeSnapshot);
  }

  /**
   * Generate a quota {@link StatsSnapshot} based on the given parameters that would have been produced by a
   * {@link com.github.ambry.store.Store}.
   * @param accountCount number of account entry in the {@link StatsSnapshot}
   * @param containerCount number of container entry in the {@link StatsSnapshot}
   * @param random the random generator to be used
   * @return the generated store level {@link StatsSnapshot}
   */
  private StatsSnapshot generateStoreStats(int accountCount, int containerCount, Random random) {
    Map<String, StatsSnapshot> accountMap = new HashMap<>();
    long totalSize = 0;
    for (int i = 0; i < accountCount; i++) {
      Map<String, StatsSnapshot> containerMap = new HashMap<>();
      long subTotalSize = 0;
      for (int j = 0; j < containerCount; j++) {
        long validSize = random.nextInt(2501) + 500;
        subTotalSize += validSize;
        containerMap.put(String.format("containerId_%d", j), new StatsSnapshot(validSize, null));
      }
      totalSize += subTotalSize;
      accountMap.put(String.format("accountId_%d", i), new StatsSnapshot(subTotalSize, containerMap));
    }
    return new StatsSnapshot(totalSize, accountMap);
  }
}
