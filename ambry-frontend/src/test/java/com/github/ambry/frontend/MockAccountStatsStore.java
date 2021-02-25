/*
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
package com.github.ambry.frontend;

import com.github.ambry.server.AccountStatsStore;
import com.github.ambry.server.StatsSnapshot;
import com.github.ambry.server.StatsWrapper;
import java.util.Map;
import java.util.Set;


/**
 * Mock implementation of {@link AccountStatsStore}.
 */
public class MockAccountStatsStore implements AccountStatsStore {
  private final String currentClusterName;
  private StatsSnapshot accountStatsSnapshot;
  private StatsSnapshot partitionClassStatsSnapshot;

  public MockAccountStatsStore(String currentClusterName) {
    this.currentClusterName = currentClusterName;
  }

  @Override
  public void storeAccountStats(StatsWrapper statsWrapper) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public void storeAggregatedAccountStats(StatsSnapshot snapshot) throws Exception {
    this.accountStatsSnapshot = snapshot;
  }

  @Override
  public StatsWrapper queryAccountStatsByHost(String hostname, int port) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, Map<String, Long>> queryAggregatedAccountStats() throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public StatsSnapshot queryAggregatedAccountStatsByClusterName(String clusterName) throws Exception {
    if (clusterName.equals(currentClusterName)) {
      return new StatsSnapshot(accountStatsSnapshot);
    } else {
      return null;
    }
  }

  @Override
  public Map<String, Map<String, Long>> queryMonthlyAggregatedAccountStats() throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public String queryRecordedMonth() throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public void takeSnapshotOfAggregatedAccountStatsAndUpdateMonth(String monthValue) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, Set<Integer>> queryPartitionNameAndIds() throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public void storePartitionClassStats(StatsWrapper statsWrapper) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public StatsWrapper queryPartitionClassStatsByHost(String hostname, int port,
      Map<String, Set<Integer>> partitionNameAndIds) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  public void storeAggregatedPartitionClassStats(StatsSnapshot statsSnapshot) throws Exception {
    this.partitionClassStatsSnapshot = statsSnapshot;
  }

  @Override
  public StatsSnapshot queryAggregatedPartitionClassStats() throws Exception {
    return null;
  }

  @Override
  public StatsSnapshot queryAggregatedPartitionClassStatsByClusterName(String clusterName) throws Exception {
    if (clusterName.equals(currentClusterName)) {
      return new StatsSnapshot(partitionClassStatsSnapshot);
    } else {
      return null;
    }
  }

  @Override
  public void closeConnection() {
  }
}
