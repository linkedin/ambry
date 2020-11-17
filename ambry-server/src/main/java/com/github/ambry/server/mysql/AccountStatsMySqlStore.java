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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.mysql.MySqlDataAccessor;
import com.github.ambry.mysql.MySqlMetrics;
import com.github.ambry.mysql.MySqlUtils;
import com.github.ambry.server.StatsSnapshot;
import com.github.ambry.server.StatsWrapper;
import java.io.File;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import joptsimple.internal.Strings;
import org.codehaus.jackson.map.ObjectMapper;


/**
 * This class publishes container storage usage to mysql. It saves previous copy of {@link StatsWrapper} and compare
 * the current {@link StatsWrapper} with the previous and only update the containers that have different storage usage.
 * It also assumes a local copy of {@link StatsWrapper} will be saved after publishing to mysql database, so it can recover
 * the previous {@link StatsWrapper} from crashing or restarting.
 */
public class AccountStatsMySqlStore {

  private final AccountReportsDao accountReportsDao;
  private StatsWrapper previousStats;
  private final Metrics storeMetrics;

  /**
   * Metrics for {@link AccountStatsMySqlStore}.
   */
  private static class Metrics {
    public final Histogram batchSize;
    public final Histogram publishTimeMs;

    /**
     * Constructor to create the Metrics.
     * @param registry The {@link MetricRegistry}.
     */
    public Metrics(MetricRegistry registry) {
      batchSize = registry.histogram(MetricRegistry.name(AccountStatsMySqlStore.class, "BatchSize"));
      publishTimeMs = registry.histogram(MetricRegistry.name(AccountStatsMySqlStore.class, "PublishTimeMs"));
    }
  }

  /**
   * Constructor to create {@link AccountStatsMySqlStore}.
   * @param dbEndpoints MySql DB end points.
   * @param localDatacenter The local datacenter name. Endpoints from local datacenter are preferred when creating connection to MySql DB.
   * @param clustername  The name of the cluster, like Ambry-prod.
   * @param hostname The name of the host.
   * @param localBackupFilePath The filepath to local backup file.
   * @param registry The {@link MetricRegistry}.
   * @throws SQLException
   */
  public AccountStatsMySqlStore(List<MySqlUtils.DbEndpoint> dbEndpoints, String localDatacenter, String clustername,
      String hostname, String localBackupFilePath, MetricRegistry registry) throws SQLException {
    this(new MySqlDataAccessor(dbEndpoints, localDatacenter, new MySqlMetrics(AccountStatsMySqlStore.class, registry)),
        clustername, hostname, localBackupFilePath, registry);
  }

  /**
   * Constructor to create link {@link AccountStatsMySqlStore}. It's only used in tests.
   * @param dataAccessor The {@link MySqlDataAccessor}.
   * @param clustername  The name of the cluster, like Ambry-prod.
   * @param hostname The name of the host.
   * @param localBackupFilePath The filepath to local backup file.
   * @param registry The {@link MetricRegistry}.
   */
  AccountStatsMySqlStore(MySqlDataAccessor dataAccessor, String clustername, String hostname,
      String localBackupFilePath, MetricRegistry registry) {
    accountReportsDao = new AccountReportsDao(dataAccessor, clustername, hostname);
    storeMetrics = new AccountStatsMySqlStore.Metrics(registry);
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

  /**
   * Publish the {@link StatsWrapper} to mysql database. This method ignores the error information from {@link StatsWrapper}
   * and only publish the container storage usages that are different from the previous one.
   * @param statsWrapper The {@link StatsWrapper} to publish.
   */
  public void publish(StatsWrapper statsWrapper) {
    if (previousStats == null) {
      applyFunctionToContainerUsageInStatsSnapshot(statsWrapper.getSnapshot(), accountReportsDao::updateStorageUsage);
    } else {
      applyFunctionToContainerUsageInDifferentStatsSnapshots(statsWrapper.getSnapshot(), previousStats.getSnapshot(),
          accountReportsDao::updateStorageUsage);
    }
    previousStats = statsWrapper;
  }

  StatsWrapper getPreviousStats() {
    return previousStats;
  }

  private void applyFunctionToContainerUsageInStatsSnapshot(StatsSnapshot statsSnapshot, ContainerUsageFunction func) {
    int batchSize = 0;
    long startTimeMs = System.currentTimeMillis();
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
          batchSize++;
        }
      }
    }
    storeMetrics.publishTimeMs.update(System.currentTimeMillis() - startTimeMs);
    storeMetrics.batchSize.update(batchSize);
  }

  private void applyFunctionToContainerUsageInDifferentStatsSnapshots(StatsSnapshot currentStats,
      StatsSnapshot previousStats, ContainerUsageFunction func) {
    int batchSize = 0;
    long startTimeMs = System.currentTimeMillis();
    Map<String, StatsSnapshot> currPartitionMap = currentStats.getSubMap();
    Map<String, StatsSnapshot> prevPartitionMap = previousStats.getSubMap();
    for (Map.Entry<String, StatsSnapshot> currPartitionMapEntry : currPartitionMap.entrySet()) {
      String partitionIdKey = currPartitionMapEntry.getKey();
      StatsSnapshot currAccountStatsSnapshot = currPartitionMapEntry.getValue();
      StatsSnapshot prevAccountStatsSnapshot =
          prevPartitionMap.getOrDefault(partitionIdKey, new StatsSnapshot((long) 0, new HashMap<>()));
      short partitionId = Short.valueOf(partitionIdKey.substring("Partition[".length(), partitionIdKey.length() - 1));
      Map<String, StatsSnapshot> currAccountMap = currAccountStatsSnapshot.getSubMap();
      Map<String, StatsSnapshot> prevAccountMap = prevAccountStatsSnapshot.getSubMap();
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
            batchSize++;
          }
        }
      }
    }
    storeMetrics.publishTimeMs.update(System.currentTimeMillis() - startTimeMs);
    storeMetrics.batchSize.update(batchSize);
  }

  @FunctionalInterface
  private interface ContainerUsageFunction {
    void apply(short partitionID, short accountId, short containerId, long storageUsage);
  }
}
