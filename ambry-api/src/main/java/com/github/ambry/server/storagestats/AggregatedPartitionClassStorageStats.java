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


/**
 * Class to represent aggregated partition class storage stats. Aggregation task would read {@link HostPartitionClassStorageStats}
 * for all hosts and aggregate them to generate an {@link AggregatedPartitionClassStorageStats}. So partition ids are not present
 * here and it's structured in this way.
 * {
 *   "PartitionClassName_1": {
 *     "AccountId_1": {
 *       "ContainerId_1": {
 *         "logicalStorageUsage": 12345,
 *         "physicalStorageUsage": 23451,
 *         "numberOfBlobs": 123
 *       },
 *       "ContainerId_2": {
 *         xxx
 *       }
 *     },
 *     "AccountId_2": {
 *       xxx
 *     }
 *   },
 *   "PartitionClassName_2": {
 *     xxx
 *   }
 * }
 */
public class AggregatedPartitionClassStorageStats {
  @JsonIgnore
  private Map<String, Map<Short, Map<Short, ContainerStorageStats>>> storageStats = new HashMap<>();

  /**
   * Constructor to instantiate an {@link AggregatedPartitionClassStorageStats}.
   * @param storageStats The storage stats map that contains all aggregated partition class storage stats.
   */
  public AggregatedPartitionClassStorageStats(Map<String, Map<Short, Map<Short, ContainerStorageStats>>> storageStats) {
    if (storageStats != null) {
      this.storageStats = storageStats;
    }
  }

  /**
   * Empty constructor for jackson
   */
  public AggregatedPartitionClassStorageStats() {
  }

  /**
   * Add partition class name and partition class storage stats in this {@link AggregatedPartitionClassStorageStats}.
   * @param partitionClassName The partition class name.
   * @param allAccountStats The partition class level (all accounts in this partition class) stats.
   */
  @JsonAnySetter
  public void add(String partitionClassName, Map<Short, Map<Short, ContainerStorageStats>> allAccountStats) {
    storageStats.put(partitionClassName, allAccountStats);
  }

  /**
   * Return aggregated partition class stats in a map.
   * @return
   */
  @JsonAnyGetter
  public Map<String, Map<Short, Map<Short, ContainerStorageStats>>> getStorageStats() {
    return Collections.unmodifiableMap(storageStats);
  }

  /**
   * Add container storage stats to this {@link AggregatedAccountStorageStats}.
   * @apram partitionClassName the partition class name.
   * @param accountId The account id
   * @param stats The container stats
   */
  public void addContainerStorageStats(String partitionClassName, short accountId, ContainerStorageStats stats) {
    storageStats.computeIfAbsent(partitionClassName, k -> new HashMap<>())
        .computeIfAbsent(accountId, k -> new HashMap<>())
        .put(stats.getContainerId(), stats);
  }
}
