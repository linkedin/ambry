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
 * Class to represent aggregated account storage stats. Aggregation task would read {@link HostAccountStorageStats} for
 * all hosts and aggregate them to generate an {@link AggregatedAccountStorageStats}. So partition ids are not present
 * here and it's structured in this way.
 * {
 *     "AccountId_1": {
 *       "ContainerId_1": {
 *         "logicalStorageUsage": 12345,
 *         "physicalStorageUsage": 23451,
 *         "numberOfBlobs": 123
 *       },
 *       "ContainerId_2": {
 *         "logicalStorageUsage": 56789
 *         "physicalStorageUsage": 67895,
 *         "numberOfBlobs": 321
 *       }
 *     },
 *     "AccountId_2": {
 *       xxx
 *     }
 * }
 */
public class AggregatedAccountStorageStats {
  @JsonIgnore
  private Map<Short, Map<Short, ContainerStorageStats>> storageStats = new HashMap<>();

  /**
   * Constructor to instantiate a {@link AggregatedAccountStorageStats}.
   * @param storageStats The storage stats map that contains all the aggregated account storage stats.
   */
  public AggregatedAccountStorageStats(Map<Short, Map<Short, ContainerStorageStats>> storageStats) {
    if (storageStats != null) {
      this.storageStats = storageStats;
    }
  }

  /**
   * Empty constructor for jackson
   */
  public AggregatedAccountStorageStats() {
  }

  /**
   * Add account id and account level storage stats in this {@link AggregatedAccountStorageStats}.
   * @param accountId The account id in string
   * @param allContainerStats The account level (all containers in this account) stats.
   */
  @JsonAnySetter
  public void add(String accountId, Map<Short, ContainerStorageStats> allContainerStats) {
    storageStats.put(Short.valueOf(accountId), allContainerStats);
  }

  /**
   * Return aggregated account stats in a map.
   * @return
   */
  @JsonAnyGetter
  public Map<Short, Map<Short, ContainerStorageStats>> getStorageStats() {
    return Collections.unmodifiableMap(storageStats);
  }

  /**
   * Add container storage stats to this {@link AggregatedAccountStorageStats}.
   * @param accountId The account id
   * @param stats The container stats
   */
  public void addContainerStorageStats(short accountId, ContainerStorageStats stats) {
    storageStats.computeIfAbsent(accountId, k -> new HashMap<>()).put(stats.getContainerId(), stats);
  }
}
