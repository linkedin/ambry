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
