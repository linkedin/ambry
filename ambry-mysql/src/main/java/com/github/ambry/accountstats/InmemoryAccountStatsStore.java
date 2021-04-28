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
package com.github.ambry.accountstats;

import com.github.ambry.server.StatsSnapshot;
import com.github.ambry.server.StatsWrapper;
import com.github.ambry.utils.Utils;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;


/**
 * An inmemory implementation of {@link AccountStatsStore}. It should be used only for tests.
 */
public class InmemoryAccountStatsStore implements AccountStatsStore {
  private final String clusterName;
  private final String hostname;

  private StatsWrapper currentHostStatsWrapper = null;
  private StatsSnapshot aggregatedAccountStats = null;
  private StatsSnapshot monthlyAggregatedAccountStats = null;
  private String currentMonth;

  private StatsSnapshot aggregatedPartitionClassStats = null;

  /**
   * Constructor to instantiate an {@link InmemoryAccountStatsStore}.
   * @param clusterName the clusterName
   * @param hostname the hostname
   */
  public InmemoryAccountStatsStore(String clusterName, String hostname) {
    this.clusterName = clusterName;
    this.hostname = hostname;
  }

  @Override
  public void storeAccountStats(StatsWrapper statsWrapper) throws Exception {
    currentHostStatsWrapper = statsWrapper;
  }

  @Override
  public void storeAggregatedAccountStats(StatsSnapshot snapshot) throws Exception {
    aggregatedAccountStats = snapshot;
  }

  @Override
  public void deleteAggregatedAccountStatsForContainer(short accountId, short containerId) throws Exception {
  }

  @Override
  public StatsWrapper queryAccountStatsByHost(String hostname, int port) throws Exception {
    if (hostname.equals(this.hostname)) {
      return currentHostStatsWrapper;
    } else {
      return null;
    }
  }

  private Map<String, Map<String, Long>> fromAggregatedAccountStats(StatsSnapshot snapshot) {
    Map<String, Map<String, Long>> result = new HashMap<>();
    for (Map.Entry<String, StatsSnapshot> accountMapEntry : snapshot.getSubMap().entrySet()) {
      String accountIdKey = accountMapEntry.getKey();
      short accountId = Utils.accountIdFromStatsAccountKey(accountIdKey);
      StatsSnapshot containerStatsSnapshot = accountMapEntry.getValue();
      for (Map.Entry<String, StatsSnapshot> currContainerMapEntry : containerStatsSnapshot.getSubMap().entrySet()) {
        String containerIdKey = currContainerMapEntry.getKey();
        short containerId = Utils.containerIdFromStatsContainerKey(containerIdKey);
        long currStorageUsage = currContainerMapEntry.getValue().getValue();
        result.computeIfAbsent(String.valueOf(accountId), k -> new HashMap<>())
            .put(String.valueOf(containerId), currStorageUsage);
      }
    }
    return result;
  }

  @Override
  public Map<String, Map<String, Long>> queryAggregatedAccountStats() throws Exception {
    return fromAggregatedAccountStats(aggregatedAccountStats);
  }

  @Override
  public StatsSnapshot queryAggregatedAccountStatsByClusterName(String clusterName) throws Exception {
    if (clusterName.equals(this.clusterName)) {
      return aggregatedAccountStats;
    } else {
      return null;
    }
  }

  @Override
  public Map<String, Map<String, Long>> queryMonthlyAggregatedAccountStats() throws Exception {
    return fromAggregatedAccountStats(monthlyAggregatedAccountStats);
  }

  @Override
  public String queryRecordedMonth() throws Exception {
    return currentMonth;
  }

  @Override
  public void takeSnapshotOfAggregatedAccountStatsAndUpdateMonth(String monthValue) throws Exception {
    currentMonth = monthValue;
    monthlyAggregatedAccountStats = aggregatedAccountStats;
  }

  @Override
  public void deleteSnapshotOfAggregatedAccountStats() throws Exception {

  }

  @Override
  public Map<String, Set<Integer>> queryPartitionNameAndIds() throws Exception {
    return null;
  }

  @Override
  public void storePartitionClassStats(StatsWrapper statsWrapper) throws Exception {

  }

  @Override
  public StatsWrapper queryPartitionClassStatsByHost(String hostname, int port,
      Map<String, Set<Integer>> partitionNameAndIds) throws Exception {
    return null;
  }

  @Override
  public void storeAggregatedPartitionClassStats(StatsSnapshot statsSnapshot) throws Exception {
    aggregatedPartitionClassStats = statsSnapshot;
  }

  @Override
  public void deleteAggregatedPartitionClassStatsForAccountContainer(String partitionClassName,
      String accountContainerKey) throws Exception {

  }

  @Override
  public StatsSnapshot queryAggregatedPartitionClassStats() throws Exception {
    return aggregatedPartitionClassStats;
  }

  @Override
  public StatsSnapshot queryAggregatedPartitionClassStatsByClusterName(String clusterName) throws Exception {
    if (clusterName.equals(this.clusterName)) {
      return aggregatedPartitionClassStats;
    } else {
      return null;
    }
  }

  @Override
  public void closeConnection() {

  }
}
