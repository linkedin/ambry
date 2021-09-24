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
import com.github.ambry.server.storagestats.HostAccountStorageStats;
import com.github.ambry.server.storagestats.HostPartitionClassStorageStats;
import com.github.ambry.utils.Utils;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
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
   * Convert a {@link StatsSnapshot} to {@link AggregatedAccountStorageStats}. We assume the given {@link StatsSnapshot}
   * follows the proper format so we can construct an {@link AggregatedAccountStorageStats} from it.
   * @param snapshot The {@link StatsSnapshot}.
   * @return The {@link AggregatedAccountStorageStats}.
   */
  public static AggregatedAccountStorageStats convertStatsSnapshotToAggregatedAccountStorageStats(
      StatsSnapshot snapshot) {
    if (snapshot == null) {
      return null;
    }
    AggregatedAccountStorageStats aggregatedAccountStorageStats = new AggregatedAccountStorageStats(null);
    Map<String, StatsSnapshot> accountMap = Optional.ofNullable(snapshot.getSubMap()).orElseGet(HashMap::new);
    for (Map.Entry<String, StatsSnapshot> accountMapEntry : accountMap.entrySet()) {
      short accountId = Utils.accountIdFromStatsAccountKey(accountMapEntry.getKey());
      Map<String, StatsSnapshot> containerMap =
          Optional.ofNullable(accountMapEntry.getValue().getSubMap()).orElseGet(HashMap::new);
      for (Map.Entry<String, StatsSnapshot> containerMapEntry : containerMap.entrySet()) {
        short containerId = Utils.containerIdFromStatsContainerKey(containerMapEntry.getKey());
        long logicalStorageUsage = containerMapEntry.getValue().getValue();
        aggregatedAccountStorageStats.addContainerStorageStats(accountId,
            new ContainerStorageStats(containerId, logicalStorageUsage, logicalStorageUsage, 0));
      }
    }
    return aggregatedAccountStorageStats;
  }

  /**
   * Convert an {@link AggregatedAccountStorageStats} to a map from account id to container id to storage usage.
   * The account id and container id are keys of the outer map and the inner map, they are both in string format.
   * @param aggregatedAccountStorageStats The {@link AggregatedAccountStorageStats}.
   * @param usePhysicalStorageUsage True to use physical storage, false to use logical storage usage.
   * @return A map from account id to container id to storage usage.
   */
  public static Map<String, Map<String, Long>> convertAggregatedAccountStorageStatsToMap(
      AggregatedAccountStorageStats aggregatedAccountStorageStats, boolean usePhysicalStorageUsage) {
    Map<String, Map<String, Long>> result = new HashMap<>();
    if (aggregatedAccountStorageStats == null) {
      return result;
    }
    Map<Short, Map<Short, ContainerStorageStats>> storageStats = aggregatedAccountStorageStats.getStorageStats();
    for (short accountId : storageStats.keySet()) {
      Map<String, Long> containerMap = storageStats.get(accountId)
          .entrySet()
          .stream()
          .collect(Collectors.toMap(ent -> String.valueOf(ent.getKey()),
              ent -> usePhysicalStorageUsage ? ent.getValue().getPhysicalStorageUsage()
                  : ent.getValue().getLogicalStorageUsage()));
      result.put(String.valueOf(accountId), containerMap);
    }
    return result;
  }

  /**
   * Convert a {@link HostAccountStorageStats} to {@link StatsSnapshot}.
   * @param hostAccountStorageStats The {@link HostAccountStorageStats}.
   * @param usePhysicalStorageUsage True to use physical storage usage, false to use logical storage usage.
   * @return The {@link StatsSnapshot}.
   */
  public static StatsSnapshot convertHostAccountStorageStatsToStatsSnapshot(
      HostAccountStorageStats hostAccountStorageStats, boolean usePhysicalStorageUsage) {
    if (hostAccountStorageStats == null) {
      return null;
    }
    Map<Long, Map<Short, Map<Short, ContainerStorageStats>>> storageStats = hostAccountStorageStats.getStorageStats();
    Map<String, StatsSnapshot> partitionSubMap = new HashMap<>();
    for (long partitionId : storageStats.keySet()) {
      Map<Short, Map<Short, ContainerStorageStats>> accountStorageStats = storageStats.get(partitionId);
      Map<String, StatsSnapshot> accountSubMap = new HashMap<>();
      for (Map.Entry<Short, Map<Short, ContainerStorageStats>> accountStorageStatsEntry : accountStorageStats.entrySet()) {
        short accountId = accountStorageStatsEntry.getKey();
        Map<String, StatsSnapshot> containerSubMap = accountStorageStatsEntry.getValue()
            .entrySet()
            .stream()
            .collect(Collectors.toMap(ent -> Utils.statsContainerKey(ent.getKey()), ent -> new StatsSnapshot(
                usePhysicalStorageUsage ? ent.getValue().getPhysicalStorageUsage()
                    : ent.getValue().getLogicalStorageUsage(), null)));
        long containerTotalValue = containerSubMap.values().stream().mapToLong(StatsSnapshot::getValue).sum();
        accountSubMap.put(Utils.statsAccountKey(accountId), new StatsSnapshot(containerTotalValue, containerSubMap));
      }
      long accountTotalValue = accountSubMap.values().stream().mapToLong(StatsSnapshot::getValue).sum();
      partitionSubMap.put(Utils.statsPartitionKey((int) partitionId),
          new StatsSnapshot(accountTotalValue, accountSubMap));
    }
    long partitionTotalValue = partitionSubMap.values().stream().mapToLong(StatsSnapshot::getValue).sum();
    return new StatsSnapshot(partitionTotalValue, partitionSubMap);
  }

  /**
   * Convert a {@link StatsSnapshot} to {@link HostAccountStorageStats}. We assume the given {@link StatsSnapshot} follows
   * the proper format so we can construct a {@link HostAccountStorageStats} from it.
   * @param snapshot The {@link StatsSnapshot}.
   * @return The {@link HostAccountStorageStats}.
   */
  public static HostAccountStorageStats convertStatsSnapshotToHostAccountStorageStats(StatsSnapshot snapshot) {
    if (snapshot == null) {
      return null;
    }
    HostAccountStorageStats hostAccountStorageStats = new HostAccountStorageStats();
    Map<String, StatsSnapshot> partitionMap =
        Optional.ofNullable(snapshot.getSubMap()).orElseGet(HashMap::new); // The map whose key is the partition id
    for (Map.Entry<String, StatsSnapshot> partitionEntry : partitionMap.entrySet()) {
      long partitionId = Utils.partitionIdFromStatsPartitionKey(partitionEntry.getKey());
      Map<String, StatsSnapshot> accountMap = Optional.ofNullable(partitionEntry.getValue().getSubMap())
          .orElseGet(HashMap::new); // The map whose key is the account id
      for (Map.Entry<String, StatsSnapshot> accountEntry : accountMap.entrySet()) {
        short accountId = Utils.accountIdFromStatsAccountKey(accountEntry.getKey());
        Map<String, StatsSnapshot> containerMap =
            Optional.ofNullable(accountEntry.getValue().getSubMap()).orElseGet(HashMap::new);
        for (Map.Entry<String, StatsSnapshot> containerEntry : containerMap.entrySet()) {
          short containerId = Utils.containerIdFromStatsContainerKey(containerEntry.getKey());
          long logicalStorageUsage = containerEntry.getValue().getValue();
          hostAccountStorageStats.addContainerStorageStats(partitionId, accountId,
              new ContainerStorageStats(containerId, logicalStorageUsage, logicalStorageUsage, 0));
        }
      }
    }
    return hostAccountStorageStats;
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

  /**
   * Convert a {@link StatsSnapshot} to an {@link AggregatedPartitionClassStorageStats}. We assume the given {@link StatsSnapshot}
   * follows the proper format so we can construct an {@link AggregatedPartitionClassStorageStats} from it.
   * @param snapshot The {@link StatsSnapshot}.
   * @return The {@link AggregatedPartitionClassStorageStats}.
   */
  public static AggregatedPartitionClassStorageStats convertStatsSnapshotToAggregatedPartitionClassStorageStats(
      StatsSnapshot snapshot) {
    if (snapshot == null) {
      return null;
    }
    AggregatedPartitionClassStorageStats aggregatedPartitionClassStorageStats =
        new AggregatedPartitionClassStorageStats(null);
    Map<String, StatsSnapshot> partitionClassNameSubMap =
        Optional.ofNullable(snapshot.getSubMap()).orElseGet(HashMap::new);
    for (Map.Entry<String, StatsSnapshot> partitionClasNameSubMapEntry : partitionClassNameSubMap.entrySet()) {
      String partitionClassName = partitionClasNameSubMapEntry.getKey();
      Map<String, StatsSnapshot> accountContainerSubMap =
          Optional.ofNullable(partitionClasNameSubMapEntry.getValue().getSubMap()).orElseGet(HashMap::new);
      for (Map.Entry<String, StatsSnapshot> accountContainerSubMapEntry : accountContainerSubMap.entrySet()) {
        short[] accountContainerIds =
            Utils.accountContainerIdFromPartitionClassStatsKey(accountContainerSubMapEntry.getKey());
        short accountId = accountContainerIds[0];
        short containerId = accountContainerIds[1];
        long logicalStorageUsage = accountContainerSubMapEntry.getValue().getValue();
        aggregatedPartitionClassStorageStats.addContainerStorageStats(partitionClassName, accountId,
            new ContainerStorageStats(containerId, logicalStorageUsage, logicalStorageUsage, 0));
      }
    }
    return aggregatedPartitionClassStorageStats;
  }

  /**
   * Convert a {@link StatsSnapshot} to a {@link HostPartitionClassStorageStats}. We assume the given {@link StatsSnapshot}
   * follows the proper format so we can construct an {@link HostPartitionClassStorageStats} from it.
   * @param snapshot The {@link StatsSnapshot}.
   * @return The {@link HostPartitionClassStorageStats}.
   */
  public static HostPartitionClassStorageStats convertStatsSnapshotToHostPartitionClassStorageStats(
      StatsSnapshot snapshot) {
    if (snapshot == null) {
      return null;
    }
    HostPartitionClassStorageStats hostPartitionClassStorageStats = new HostPartitionClassStorageStats();
    Map<String, StatsSnapshot> partitionClassSubMap = Optional.ofNullable(snapshot.getSubMap()).orElseGet(HashMap::new);
    for (Map.Entry<String, StatsSnapshot> partitionClassSubMapEntry : partitionClassSubMap.entrySet()) {
      String partitionClassName = partitionClassSubMapEntry.getKey();
      Map<String, StatsSnapshot> partitionSubMap =
          Optional.ofNullable(partitionClassSubMapEntry.getValue().getSubMap()).orElseGet(HashMap::new);
      for (Map.Entry<String, StatsSnapshot> partitionSubMapEntry : partitionSubMap.entrySet()) {
        long partitionId = Utils.partitionIdFromStatsPartitionKey(partitionSubMapEntry.getKey());
        Map<String, StatsSnapshot> accountContainerSubMap =
            Optional.ofNullable(partitionSubMapEntry.getValue().getSubMap()).orElseGet(HashMap::new);
        for (Map.Entry<String, StatsSnapshot> accountContainerSubMapEntry : accountContainerSubMap.entrySet()) {
          short[] accountContainerIds =
              Utils.accountContainerIdFromPartitionClassStatsKey(accountContainerSubMapEntry.getKey());
          short accountId = accountContainerIds[0];
          short containerId = accountContainerIds[1];
          long logicalStorageUsage = accountContainerSubMapEntry.getValue().getValue();
          hostPartitionClassStorageStats.addContainerStorageStats(partitionClassName, partitionId, accountId,
              new ContainerStorageStats(containerId, logicalStorageUsage, logicalStorageUsage, 0));
        }
      }
    }
    return hostPartitionClassStorageStats;
  }

  /**
   * Convert a {@link HostPartitionClassStorageStats} to a {@link StatsSnapshot}.
   * @param hostPartitionClassStorageStats The {@link HostPartitionClassStorageStats}.
   * @param usePhysicalStorageUsage True to use physical storage usage, false to use logical storage usage.
   * @return The {@link StatsSnapshot}
   */
  public static StatsSnapshot convertHostPartitionClassStorageStatsToStatsSnapshot(
      HostPartitionClassStorageStats hostPartitionClassStorageStats, boolean usePhysicalStorageUsage) {
    if (hostPartitionClassStorageStats == null) {
      return null;
    }
    Map<String, Map<Long, Map<Short, Map<Short, ContainerStorageStats>>>> storageStats =
        hostPartitionClassStorageStats.getStorageStats();
    Map<String, StatsSnapshot> partitionClassNameSubMap = new HashMap<>();
    for (String partitionClassName : storageStats.keySet()) {
      Map<Long, Map<Short, Map<Short, ContainerStorageStats>>> partitionStorageStats =
          storageStats.get(partitionClassName);
      Map<String, StatsSnapshot> partitionSubMap = new HashMap<>();
      for (long partitionId : partitionStorageStats.keySet()) {
        Map<Short, Map<Short, ContainerStorageStats>> accountStorageStats = partitionStorageStats.get(partitionId);
        Map<String, StatsSnapshot> accountContainerSubMap = new HashMap<>();
        for (Map.Entry<Short, Map<Short, ContainerStorageStats>> accountStorageStatsEntry : accountStorageStats.entrySet()) {
          short accountId = accountStorageStatsEntry.getKey();
          accountStorageStatsEntry.getValue()
              .values()
              .forEach(containerStats -> accountContainerSubMap.put(
                  Utils.partitionClassStatsAccountContainerKey(accountId, containerStats.getContainerId()),
                  new StatsSnapshot(usePhysicalStorageUsage ? containerStats.getPhysicalStorageUsage()
                      : containerStats.getLogicalStorageUsage(), null)));
        }
        long accountContainerTotalValue =
            accountContainerSubMap.values().stream().mapToLong(StatsSnapshot::getValue).sum();
        partitionSubMap.put(Utils.statsPartitionKey((int) partitionId),
            new StatsSnapshot(accountContainerTotalValue, accountContainerSubMap));
      }
      long partitionTotalValue = partitionSubMap.values().stream().mapToLong(StatsSnapshot::getValue).sum();
      partitionClassNameSubMap.put(partitionClassName, new StatsSnapshot(partitionTotalValue, partitionSubMap));
    }
    long partitionClassNameTotalValue =
        partitionClassNameSubMap.values().stream().mapToLong(StatsSnapshot::getValue).sum();
    return new StatsSnapshot(partitionClassNameTotalValue, partitionClassNameSubMap);
  }
}
