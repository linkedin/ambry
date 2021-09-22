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

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * Class to represent account storage stats for a given host. It's structured in this way.
 * {
 *     "PartitionId_1":  {
 *       "AccountId_1": {
 *         "ContainerId_1": {
 *           "logicalStorageUsage": 12345,
 *           "physicalStorageUsage": 23451,
 *           "numberOfBlobs": 123
 *         },
 *         "ContainerId_2": {
 *           "logicalStorageUsage": 56789
 *           "physicalStorageUsage": 67895,
 *           "numberOfBlobs": 321
 *         }
 *       },
 *       "AccountId_2": {
 *         xxx
 *       }
 *     },
 *     "PartitionId_2": {
 *       xxx
 *     }
 * }
 */
public class HostAccountStorageStats {
  @JsonIgnore
  private Map<Long, Map<Short, Map<Short, ContainerStorageStats>>> storageStats = new HashMap<>();

  /**
   * Constructor to instantiate a {@link HostAccountStorageStats}.
   * @param storageStats The storage stats map that contains all partition level storage stats.
   */
  public HostAccountStorageStats(Map<Long, Map<Short, Map<Short, ContainerStorageStats>>> storageStats) {
    if (storageStats != null) {
      this.storageStats = storageStats;
    }
  }

  /**
   * Empty constructor for Jackson
   */
  public HostAccountStorageStats() {
  }

  /**
   * This is a copy constructor.
   * @param other
   */
  public HostAccountStorageStats(HostAccountStorageStats other) {
    Map<Long, Map<Short, Map<Short, ContainerStorageStats>>> storageStats = other.getStorageStats();
    for (long partitionId : storageStats.keySet()) {
      Map<Short, Map<Short, ContainerStorageStats>> accountStorageStatsMap = storageStats.get(partitionId);
      this.storageStats.put(partitionId, new HashMap<>());
      for (short accountId : accountStorageStatsMap.keySet()) {
        Map<Short, ContainerStorageStats> containerStorageStatsMap = accountStorageStatsMap.get(accountId);
        this.storageStats.get(partitionId)
            .put(accountId, containerStorageStatsMap.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, ent -> new ContainerStorageStats(ent.getValue()))));
      }
    }
  }

  /**
   * Add partition id and partition level storage stats in this {@link HostAccountStorageStats}.
   * @param partitionId The partition id
   * @param allAccountStats The partition level (all accounts in this partition) stats.
   */
  @JsonAnySetter
  public void add(String partitionId, Map<Short, Map<Short, ContainerStorageStats>> allAccountStats) {
    storageStats.put(Long.valueOf(partitionId), allAccountStats);
  }

  /**
   * Return partition level storage stats.
   * @return
   */
  @JsonAnyGetter
  public Map<Long, Map<Short, Map<Short, ContainerStorageStats>>> getStorageStats() {
    return Collections.unmodifiableMap(storageStats);
  }

  /**
   * Add container storage stats to this {@link HostAccountStorageStats}.
   * @param partitionId The partition id
   * @param accountId The account id
   * @param stats The container stats
   */
  public void addContainerStorageStats(long partitionId, short accountId, ContainerStorageStats stats) {
    storageStats.computeIfAbsent(partitionId, k -> new HashMap<>())
        .computeIfAbsent(accountId, k -> new HashMap<>())
        .put(stats.getContainerId(), stats);
  }
}
