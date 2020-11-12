/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.server.mysql;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.mysql.MySqlDataAccessor;
import com.github.ambry.mysql.MySqlMetrics;
import com.github.ambry.mysql.MySqlUtils;
import com.github.ambry.server.StatsReporter;
import com.github.ambry.server.StatsSnapshot;
import com.github.ambry.server.StatsWrapper;
import java.io.File;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import joptsimple.internal.Strings;
import org.codehaus.jackson.map.ObjectMapper;


public class AccountStatsMySqlStore implements StatsReporter {
  private final AccountReportsDao accountReportsDao;
  private StatsWrapper previousStats;

  public AccountStatsMySqlStore(List<MySqlUtils.DbEndpoint> dbEndpoints, String localDatacenter, String clustername,
      String hostname, String localBackupFilePath, MetricRegistry registry) throws SQLException {
    MySqlMetrics metrics = new MySqlMetrics(AccountStatsMySqlStore.class, registry);
    MySqlDataAccessor mySqlDataAccessor = new MySqlDataAccessor(dbEndpoints, localDatacenter, metrics);
    accountReportsDao = new AccountReportsDao(mySqlDataAccessor, clustername, hostname);
    if (!Strings.isNullOrEmpty(localBackupFilePath)) {
      // load backup file and this backup is the previous stats
      ObjectMapper objectMapper = new ObjectMapper();
      try {
        this.previousStats = objectMapper.readValue(new File(localBackupFilePath), StatsWrapper.class);
      } catch (Exception e) {
        this.previousStats = null;
      }
    }
  }

  public void publish(StatsWrapper statsWrapper) {
    if (previousStats == null) {
      applyFunctionToContainerUsageInStatsSnapshot(statsWrapper.getSnapshot(), accountReportsDao::updateStorageUsage);
    } else {
      applyFunctionToContainerUsageInDifferentStatsSnapshots(statsWrapper.getSnapshot(), previousStats.getSnapshot(),
          accountReportsDao::updateStorageUsage);
    }
    previousStats = statsWrapper;
  }

  private void applyFunctionToContainerUsageInStatsSnapshot(StatsSnapshot statsSnapshot, ContainerUsageFunction func) {
    // StatsSnapshot has three levels, Parition -> Account -> Container
    Map<String, StatsSnapshot> partitionMap = statsSnapshot.getSubMap();
    for (Map.Entry<String, StatsSnapshot> partitionMapEntry : partitionMap.entrySet()) {
      String partitionIdKey = partitionMapEntry.getKey();
      StatsSnapshot accountStatsSnapshot = partitionMapEntry.getValue();
      short partitionId = Short.valueOf(partitionIdKey.substring("Partition[".length(), partitionIdKey.length() - 1));
      Map<String, StatsSnapshot> accountMap = accountStatsSnapshot.getSubMap();
      for (Map.Entry<String, StatsSnapshot> accountMapEntry : accountMap.entrySet()) {
        String accountIdKey = accountMapEntry.getKey();
        StatsSnapshot containerStatsSnapshot = accountMapEntry.getValue();
        short accountId = Short.valueOf(accountIdKey.substring(2, accountIdKey.length() - 1));
        Map<String, StatsSnapshot> containerMap = containerStatsSnapshot.getSubMap();
        for (Map.Entry<String, StatsSnapshot> containerMapEntry : containerMap.entrySet()) {
          String containerIdKey = containerMapEntry.getKey();
          short containerId = Short.valueOf(containerIdKey.substring(2, containerIdKey.length() - 1));
          long storageUsage = containerMapEntry.getValue().getValue();
          func.apply(partitionId, accountId, containerId, storageUsage);
        }
      }
    }
  }

  private void applyFunctionToContainerUsageInDifferentStatsSnapshots(StatsSnapshot currentStats,
      StatsSnapshot previousStats, ContainerUsageFunction func) {
    Map<String, StatsSnapshot> currPartitionMap = currentStats.getSubMap();
    Map<String, StatsSnapshot> prevPartitionMap = previousStats.getSubMap();
    for (Map.Entry<String, StatsSnapshot> currPartitionMapEntry : currPartitionMap.entrySet()) {
      String partitionIdKey = currPartitionMapEntry.getKey();
      StatsSnapshot currAccountStatsSnapshot = currPartitionMapEntry.getValue();
      StatsSnapshot prevAccountStatsSnapshot =
          prevPartitionMap.getOrDefault(partitionIdKey, new StatsSnapshot((long) 0, new HashMap<>()));
      short partitionId = Short.valueOf(partitionIdKey.substring("Partition[".length(), partitionIdKey.length() - 1));
      Map<String, StatsSnapshot> currAccountMap = currAccountStatsSnapshot.getSubMap();
      Map<String, StatsSnapshot> prevAccountMap = currAccountStatsSnapshot.getSubMap();
      for (Map.Entry<String, StatsSnapshot> currAccountMapEntry : currAccountMap.entrySet()) {
        String accountIdKey = currAccountMapEntry.getKey();
        StatsSnapshot currContainerStatsSnapshot = currAccountMapEntry.getValue();
        StatsSnapshot prevContainerStatsSnapshot =
            prevAccountMap.getOrDefault(accountIdKey, new StatsSnapshot((long) 0, new HashMap<>()));
        short accountId = Short.valueOf(accountIdKey.substring(2, accountIdKey.length() - 1));
        Map<String, StatsSnapshot> currContainerMap = currContainerStatsSnapshot.getSubMap();
        Map<String, StatsSnapshot> prevContainerMap = prevContainerStatsSnapshot.getSubMap();
        for (Map.Entry<String, StatsSnapshot> currContainerMapEntry : currContainerMap.entrySet()) {
          String containerIdKey = currContainerMapEntry.getKey();
          short containerId = Short.valueOf(containerIdKey.substring(2, containerIdKey.length() - 1));
          long currStorageUsage = currContainerMapEntry.getValue().getValue();
          long prevStorageUsage =
              prevContainerMap.getOrDefault(containerIdKey, new StatsSnapshot((long) -1, null)).getValue();
          if (currStorageUsage != prevStorageUsage) {
            func.apply(partitionId, accountId, containerId, currStorageUsage);
          }
        }
      }
    }
  }

  @FunctionalInterface
  private interface ContainerUsageFunction {
    void apply(short partitionID, short accountId, short containerId, long storageUsage);
  }
}
