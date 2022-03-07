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

import com.github.ambry.server.storagestats.ContainerStorageStats;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;


/**
 * Utils functions for any tests related to storage stats.
 */
public class StorageStatsUtilTest {
  private static final Random random = new Random();

  /**
   * Generate aggregated account storage stats in a map with random values.
   * @param accountIdStart The start value of account id
   * @param numberOfAccounts The number of accounts
   * @param numberOfContainersPerAccount The number of containers per account
   * @param maxLogicalStorageUsage the maximum value for logical storage usage
   * @param physicalFactor The multiply factor for physical storage usage
   * @param maxNumberOfBlobs The maximum value for number of blobs.
   * @return
   */
  public static Map<Short, Map<Short, ContainerStorageStats>> generateRandomAggregatedAccountStorageStats(
      short accountIdStart, int numberOfAccounts, int numberOfContainersPerAccount, long maxLogicalStorageUsage,
      int physicalFactor, int maxNumberOfBlobs) {
    short accountId = accountIdStart;
    Map<Short, Map<Short, ContainerStorageStats>> result = new HashMap<>();
    for (int i = 0; i < numberOfAccounts; i++) {
      if (!result.containsKey(accountId)) {
        result.put(accountId, new HashMap<>());
      }
      short containerId = 0;
      for (int j = 0; j < numberOfContainersPerAccount; j++) {
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
        containerId++;
      }
      accountId++;
    }
    return result;
  }

  /**
   * Generate host account storage stats in a map with random values.
   * @param numberOfPartitions The number of partitions
   * @param numberOfAccounts The number of accounts
   * @param numberOfContainersPerAccount The number of containers per account
   * @param maxLogicalStorageUsage the maximum value for logical storage usage
   * @param physicalFactor The multiply factor for physical storage usage
   * @param maxNumberOfBlobs The maximum value for number of blobs.
   * @return
   */
  public static Map<Long, Map<Short, Map<Short, ContainerStorageStats>>> generateRandomHostAccountStorageStats(
      int numberOfPartitions, int numberOfAccounts, int numberOfContainersPerAccount, long maxLogicalStorageUsage,
      int physicalFactor, int maxNumberOfBlobs) {
    Map<Long, Map<Short, Map<Short, ContainerStorageStats>>> result = new HashMap<>();
    for (int i = 0; i < numberOfPartitions; i++) {
      result.put((long) i,
          generateRandomAggregatedAccountStorageStats((short) 0, numberOfAccounts, numberOfContainersPerAccount,
              maxLogicalStorageUsage, physicalFactor, maxNumberOfBlobs));
    }
    return result;
  }

  /**
   * Generate aggregated partition class storage stats in a map with random values.
   * @param partitionClassNames The array of the partition class names
   * @param accountIdStart  The start value of account id
   * @param numberOfAccounts The number of accounts
   * @param numberOfContainersPerAccount The number of containers per account
   * @param maxLogicalStorageUsage the maximum value for logical storage usage
   * @param physicalFactor The multiply factor for physical storage usage
   * @param maxNumberOfBlobs The maximum value for number of blobs.
   * @return
   */
  public static Map<String, Map<Short, Map<Short, ContainerStorageStats>>> generateRandomAggregatedPartitionClassStorageStats(
      String[] partitionClassNames, short accountIdStart, int numberOfAccounts, int numberOfContainersPerAccount,
      long maxLogicalStorageUsage, int physicalFactor, int maxNumberOfBlobs) {
    Map<String, Map<Short, Map<Short, ContainerStorageStats>>> storageStats = new HashMap<>();
    for (String partitionClassName : partitionClassNames) {
      storageStats.put(partitionClassName,
          generateRandomAggregatedAccountStorageStats(accountIdStart, numberOfAccounts, numberOfContainersPerAccount,
              maxLogicalStorageUsage, physicalFactor, maxNumberOfBlobs));
    }
    return storageStats;
  }
}
