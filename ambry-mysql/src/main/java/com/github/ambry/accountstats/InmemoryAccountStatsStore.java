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

import com.github.ambry.server.HostAccountStorageStatsWrapper;
import com.github.ambry.server.HostPartitionClassStorageStatsWrapper;
import com.github.ambry.server.storagestats.AggregatedAccountStorageStats;
import com.github.ambry.server.storagestats.AggregatedPartitionClassStorageStats;
import java.util.Map;
import java.util.Set;


/**
 * An inmemory implementation of {@link AccountStatsStore}. It should be used only for tests.
 */
public class InmemoryAccountStatsStore implements AccountStatsStore {
  private final String clusterName;
  private final String hostname;

  private HostAccountStorageStatsWrapper currentHostStatsWrapper = null;
  private AggregatedAccountStorageStats aggregatedAccountStats = null;
  private AggregatedAccountStorageStats monthlyAggregatedAccountStats = null;
  private String currentMonth;

  private AggregatedPartitionClassStorageStats aggregatedPartitionClassStats = null;

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
  public void storeHostAccountStorageStats(HostAccountStorageStatsWrapper statsWrapper) throws Exception {
    currentHostStatsWrapper = statsWrapper;
  }

  @Override
  public void deleteHostAccountStorageStatsForHost(String hostname, int port) throws Exception {
    if (hostname.equals(this.hostname)) {
      currentHostStatsWrapper = null;
    }
  }

  @Override
  public void storeAggregatedAccountStorageStats(AggregatedAccountStorageStats aggregatedAccountStorageStats)
      throws Exception {
    aggregatedAccountStats = aggregatedAccountStorageStats;
  }

  @Override
  public void deleteAggregatedAccountStatsForContainer(short accountId, short containerId) throws Exception {
  }

  @Override
  public HostAccountStorageStatsWrapper queryHostAccountStorageStatsByHost(String hostname, int port) throws Exception {
    if (hostname.equals(this.hostname)) {
      return currentHostStatsWrapper;
    } else {
      return null;
    }
  }

  @Override
  public AggregatedAccountStorageStats queryAggregatedAccountStorageStats() throws Exception {
    return aggregatedAccountStats;
  }

  @Override
  public AggregatedAccountStorageStats queryAggregatedAccountStorageStatsByClusterName(String clusterName)
      throws Exception {
    if (clusterName.equals(this.clusterName)) {
      return aggregatedAccountStats;
    } else {
      return null;
    }
  }

  @Override
  public AggregatedAccountStorageStats queryMonthlyAggregatedAccountStorageStats() throws Exception {
    return monthlyAggregatedAccountStats;
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
  public void storeHostPartitionClassStorageStats(HostPartitionClassStorageStatsWrapper statsWrapper) throws Exception {

  }

  @Override
  public HostPartitionClassStorageStatsWrapper queryHostPartitionClassStorageStatsByHost(String hostname, int port,
      Map<String, Set<Integer>> partitionNameAndIds) throws Exception {
    return null;
  }

  @Override
  public void storeAggregatedPartitionClassStorageStats(AggregatedPartitionClassStorageStats stats) throws Exception {
    aggregatedPartitionClassStats = stats;
  }

  @Override
  public void deleteAggregatedPartitionClassStatsForAccountContainer(String partitionClassName, short accountId,
      short containerId) throws Exception {

  }

  @Override
  public AggregatedPartitionClassStorageStats queryAggregatedPartitionClassStorageStats() throws Exception {
    return aggregatedPartitionClassStats;
  }

  @Override
  public AggregatedPartitionClassStorageStats queryAggregatedPartitionClassStorageStatsByClusterName(String clusterName)
      throws Exception {
    if (clusterName.equals(this.clusterName)) {
      return aggregatedPartitionClassStats;
    } else {
      return null;
    }
  }

  @Override
  public void shutdown() {

  }
}
