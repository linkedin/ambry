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
package com.github.ambry.server.storagestats;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * Class to represent partition class storage stats for a given host. It's structured in this way.
 * {
 *   "PartitionClassName_1": {
 *     "PartitionId_1": {
 *       "AccountId_1": {
 *         "ContainerId_1": {
 *           "logicalStorageUsage": 12345,
 *           "physicalStorageUsage": 23451,
 *           "numberOfBlobs": 123
 *         }
 *       },
 *       "AccountId_2": {
 *         xxx
 *       }
 *     },
 *     "PartitionId_2": {
 *       xxx
 *     }
 *   },
 *   "PartitionClassName_2": {
 *     xxx
 *   }
 * }
 */
public class HostPartitionClassStorageStats {
  private Map<String, Map<Long, Map<Short, Map<Short, ContainerStorageStats>>>> storageStats = new HashMap<>();

  public HostPartitionClassStorageStats() {
  }

  /**
   * Constructor to instantiate a {@link HostPartitionClassStorageStats}.
   * @param storageStats The storage stats map that contains all partition class level storage stats.
   */
  public HostPartitionClassStorageStats(
      Map<String, Map<Long, Map<Short, Map<Short, ContainerStorageStats>>>> storageStats) {
    if (storageStats != null) {
      this.storageStats = storageStats;
    }
  }

  /**
   * This is a copy constructor
   * @param other
   */
  public HostPartitionClassStorageStats(HostPartitionClassStorageStats other) {
    Map<String, Map<Long, Map<Short, Map<Short, ContainerStorageStats>>>> storageStats = other.getStorageStats();
    for (String partitionClassName : storageStats.keySet()) {
      this.storageStats.put(partitionClassName, new HashMap<>());
      for (long partitionId : storageStats.get(partitionClassName).keySet()) {
        Map<Short, Map<Short, ContainerStorageStats>> accountStorageStatsMap =
            storageStats.get(partitionClassName).get(partitionId);
        this.storageStats.get(partitionClassName).put(partitionId, new HashMap<>());
        for (short accountId : accountStorageStatsMap.keySet()) {
          Map<Short, ContainerStorageStats> containerStorageStatsMap = accountStorageStatsMap.get(accountId);
          this.storageStats.get(partitionClassName)
              .get(partitionId)
              .put(accountId, containerStorageStatsMap.entrySet()
                  .stream()
                  .collect(Collectors.toMap(Map.Entry::getKey, ent -> new ContainerStorageStats(ent.getValue()))));
        }
      }
    }
  }

  /**
   * Return partition class level storage stats.
   * @return
   */
  public Map<String, Map<Long, Map<Short, Map<Short, ContainerStorageStats>>>> getStorageStats() {
    return Collections.unmodifiableMap(storageStats);
  }

  /**
   * Add container storage stats to this {@link HostPartitionClassStorageStats}.
   * @param partitionClassName The partition class name
   * @param partitionId The partition id
   * @param accountId The account id
   * @param stats The container stats
   */
  public void addContainerStorageStats(String partitionClassName, long partitionId, short accountId,
      ContainerStorageStats stats) {
    storageStats.computeIfAbsent(partitionClassName, k -> new HashMap<>())
        .computeIfAbsent(partitionId, k -> new HashMap<>())
        .computeIfAbsent(accountId, k -> new HashMap<>())
        .put(stats.getContainerId(), stats);
  }
}
