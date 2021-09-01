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
import com.github.ambry.server.storagestats.AggregatedPartitionClassStorageStats;
import com.github.ambry.server.storagestats.ContainerStorageStats;
import com.github.ambry.utils.Utils;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * Utility class that convert new storage stats object to {@link StatsSnapshot} object.
 */
public class StorageStatsUtil {

  /**
   * Convert an {@link AggregatedAccountStorageStats} to {@link StatsSnapshot}.
   * @param aggregatedAccountStorageStats The {@link AggregatedAccountStorageStats}.
   * @param usePhysicalStorageUsage True to use physical storage usage, false to use logical storage usage.
   * @return The {@link StatsSnapshot}.
   */
  public static StatsSnapshot convertAggregatedAccountStorageStatsToStatsSnapshot(
      AggregatedAccountStorageStats aggregatedAccountStorageStats, boolean usePhysicalStorageUsage) {
    if (aggregatedAccountStorageStats == null) {
      return null;
    }
    Map<Short, Map<Short, ContainerStorageStats>> storageStats = aggregatedAccountStorageStats.getStorageStats();
    Map<String, StatsSnapshot> accountSubMap = new HashMap<>();
    for (Short accountId : storageStats.keySet()) {
      Map<String, StatsSnapshot> containerSubMap = storageStats.get(accountId)
          .entrySet()
          .stream()
          .collect(Collectors.toMap(ent -> Utils.statsContainerKey(ent.getKey()), ent -> new StatsSnapshot(
              usePhysicalStorageUsage ? ent.getValue().getPhysicalStorageUsage()
                  : ent.getValue().getLogicalStorageUsage(), null)));
      long containerValue = containerSubMap.values().stream().mapToLong(StatsSnapshot::getValue).sum();
      accountSubMap.put(Utils.statsAccountKey(accountId), new StatsSnapshot(containerValue, containerSubMap));
    }
    long accountValue = accountSubMap.values().stream().mapToLong(StatsSnapshot::getValue).sum();
    return new StatsSnapshot(accountValue, accountSubMap);
  }

  /**
   * Convert an {@link AggregatedPartitionClassStorageStats} to {@link StatsSnapshot}.
   * @param aggregatedPartitionClassStorageStats The {@link AggregatedPartitionClassStorageStats}.
   * @param usePhysicalStorageUsage True to use physical storage usage, false to use logical storage usage.
   * @return The {@link StatsSnapshot}.
   */
  public static StatsSnapshot convertAggregatedPartitionClassStorageStatsToStatsSnapshot(
      AggregatedPartitionClassStorageStats aggregatedPartitionClassStorageStats, boolean usePhysicalStorageUsage) {
    if (aggregatedPartitionClassStorageStats == null) {
      return null;
    }
    Map<String, Map<Short, Map<Short, ContainerStorageStats>>> storageStats =
        aggregatedPartitionClassStorageStats.getStorageStats();
    Map<String, StatsSnapshot> partitionClassNameSubMap = new HashMap<>();
    for (String partitionClassName : storageStats.keySet()) {
      Map<String, StatsSnapshot> accountContainerSubMap = new HashMap<>();
      for (short accountId : storageStats.get(partitionClassName).keySet()) {
        for (Map.Entry<Short, ContainerStorageStats> containerEntry : storageStats.get(partitionClassName)
            .get(accountId)
            .entrySet()) {
          short containerId = containerEntry.getKey();
          long usage = usePhysicalStorageUsage ? containerEntry.getValue().getPhysicalStorageUsage()
              : containerEntry.getValue().getLogicalStorageUsage();
          accountContainerSubMap.put(Utils.partitionClassStatsAccountContainerKey(accountId, containerId),
              new StatsSnapshot(usage, null));
        }
      }
      long accountContainerValue = accountContainerSubMap.values().stream().mapToLong(StatsSnapshot::getValue).sum();
      partitionClassNameSubMap.put(partitionClassName,
          new StatsSnapshot(accountContainerValue, accountContainerSubMap));
    }
    long partitionClassNameValue = partitionClassNameSubMap.values().stream().mapToLong(StatsSnapshot::getValue).sum();
    return new StatsSnapshot(partitionClassNameValue, partitionClassNameSubMap);
  }
}
