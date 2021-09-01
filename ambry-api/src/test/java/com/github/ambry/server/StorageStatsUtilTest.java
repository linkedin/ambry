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
package com.github.ambry.server;

import com.github.ambry.server.storagestats.AggregatedAccountStorageStats;
import com.github.ambry.server.storagestats.ContainerStorageStats;
import com.github.ambry.utils.TestUtils;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import junit.framework.Assert;
import org.junit.Test;


/**
 * Unit tests for {@link StorageStatsUtil}.
 */
public class StorageStatsUtilTest {
  private static final Random random = new Random();

  /**
   * Test case for {@link StorageStatsUtil#convertAggregatedAccountStorageStatsToStatsSnapshot}.
   */
  @Test
  public void testAggregatedAccountStorageStatsConverter() {
    Map<Short, Map<Short, ContainerStorageStats>> storageStats =
        generateRandomAggregatedAccountStorageStats(10, 10, 10000L, 2, 10);
    AggregatedAccountStorageStats aggregatedAccountStorageStats = new AggregatedAccountStorageStats(storageStats);

    StatsSnapshot expected = TestUtils.makeAccountStatsSnapshotFromContainerStorageMap(
        convertAggregatedAccountStorageStatsMapToContainerStorageMap(storageStats, false));
    StatsSnapshot snapshot =
        StorageStatsUtil.convertAggregatedAccountStorageStatsToStatsSnapshot(aggregatedAccountStorageStats, false);
    Assert.assertEquals(expected, snapshot);

    expected = TestUtils.makeAccountStatsSnapshotFromContainerStorageMap(
        convertAggregatedAccountStorageStatsMapToContainerStorageMap(storageStats, true));
    snapshot =
        StorageStatsUtil.convertAggregatedAccountStorageStatsToStatsSnapshot(aggregatedAccountStorageStats, true);
    Assert.assertEquals(expected, snapshot);
  }

  /**
   * Generate account storage stats in a map with random values.
   * @param numberOfAccounts The number of accounts
   * @param numberOfContainersPerAccount The number of containers per account
   * @param maxLogicalStorageUsage the maximum value for logical storage usage
   * @param physicalFactor The multiply factor for physical storage usage
   * @param maxNumberOfBlobs The maximum value for number of blobs.
   * @return
   */
  static Map<Short, Map<Short, ContainerStorageStats>> generateRandomAggregatedAccountStorageStats(int numberOfAccounts,
      int numberOfContainersPerAccount, long maxLogicalStorageUsage, int physicalFactor, int maxNumberOfBlobs) {
    short accountId = 1000;
    short containerId = 1;
    Map<Short, Map<Short, ContainerStorageStats>> result = new HashMap<>();
    for (int i = 0; i < numberOfAccounts; i++) {
      accountId++;
      if (!result.containsKey(accountId)) {
        result.put(accountId, new HashMap<>());
      }
      for (int j = 0; j < numberOfContainersPerAccount; j++) {
        containerId++;
        long logicalStorageUsage = Math.abs(random.nextLong() % maxLogicalStorageUsage);
        int numberOfBlobs = random.nextInt(maxNumberOfBlobs);
        if (numberOfBlobs == 0) {
          numberOfBlobs = 1;
        }
        result.get(accountId)
            .put(containerId, new ContainerStorageStats.Builder(containerId).logicalStorageUsage(logicalStorageUsage)
                .physicalStorageUsage(logicalStorageUsage * physicalFactor)
                .numberOfBlobs(numberOfBlobs)
                .build());
      }
    }
    return result;
  }

  /**
   * Convert aggregated account storage stats in a map to container storage map so we can use the converted map to generate
   * a {@link StatsSnapshot}.
   * @param storageStats The account storage stats.
   * @param usePhysicalStorageUsage True to use physical storage usage, otherwise, use logical storage usage.
   * @return
   */
  static Map<String, Map<String, Long>> convertAggregatedAccountStorageStatsMapToContainerStorageMap(
      Map<Short, Map<Short, ContainerStorageStats>> storageStats, boolean usePhysicalStorageUsage) {
    Map<String, Map<String, Long>> result = new HashMap<>();
    for (short accountId : storageStats.keySet()) {
      result.put(String.valueOf(accountId), new HashMap<>());
      for (short containerId : storageStats.get(accountId).keySet()) {
        ContainerStorageStats stats = storageStats.get(accountId).get(containerId);
        result.get(String.valueOf(accountId))
            .put(String.valueOf(containerId),
                usePhysicalStorageUsage ? stats.getPhysicalStorageUsage() : stats.getLogicalStorageUsage());
      }
    }
    return result;
  }
}
