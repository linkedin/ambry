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
import com.github.ambry.server.AccountStatsStore;
import com.github.ambry.server.StatsHeader;
import com.github.ambry.server.StatsSnapshot;
import com.github.ambry.server.StatsWrapper;
import java.io.File;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import joptsimple.internal.Strings;
import org.codehaus.jackson.map.ObjectMapper;


/**
 * This class publishes container storage usage to mysql. It saves previous copy of {@link StatsWrapper} and compare
 * the current {@link StatsWrapper} with the previous and only update the containers that have different storage usage.
 * It also assumes a local copy of {@link StatsWrapper} will be saved after publishing to mysql database, so it can recover
 * the previous {@link StatsWrapper} from crashing or restarting.
 */
public class AccountStatsMySqlStore implements AccountStatsStore {

  private final MySqlDataAccessor mySqlDataAccessor;
  private final AccountReportsDao accountReportsDao;
  private final AggregatedAccountReportsDao aggregatedaccountReportsDao;
  private final HostnameHelper hostnameHelper;
  private StatsWrapper previousStats;
  private final Metrics storeMetrics;

  /**
   * Metrics for {@link AccountStatsMySqlStore}.
   */
  private static class Metrics {
    public final Histogram batchSize;
    public final Histogram publishTimeMs;
    public final Histogram aggregatedBatchSize;
    public final Histogram aggregatedPublishTimeMs;

    /**
     * Constructor to create the Metrics.
     * @param registry The {@link MetricRegistry}.
     */
    public Metrics(MetricRegistry registry) {
      batchSize = registry.histogram(MetricRegistry.name(AccountStatsMySqlStore.class, "BatchSize"));
      publishTimeMs = registry.histogram(MetricRegistry.name(AccountStatsMySqlStore.class, "PublishTimeMs"));
      aggregatedBatchSize =
          registry.histogram(MetricRegistry.name(AccountStatsMySqlStore.class, "AggregatedBatchSize"));
      aggregatedPublishTimeMs =
          registry.histogram(MetricRegistry.name(AccountStatsMySqlStore.class, "AggregatedPublishTimeMs"));
    }
  }

  /**
   * Constructor to create {@link AccountStatsMySqlStore}.
   * @param dbEndpoints MySql DB end points.
   * @param localDatacenter The local datacenter name. Endpoints from local datacenter are preferred when creating connection to MySql DB.
   * @param clusterName  The name of the cluster, like Ambry-test.
   * @param hostname The name of the host.
   * @param localBackupFilePath The filepath to local backup file.
   * @param hostnameHelper The {@link HostnameHelper} to simplify the hostname.
   * @param registry The {@link MetricRegistry}.
   * @throws SQLException
   */
  public AccountStatsMySqlStore(List<MySqlUtils.DbEndpoint> dbEndpoints, String localDatacenter, String clusterName,
      String hostname, String localBackupFilePath, HostnameHelper hostnameHelper, MetricRegistry registry)
      throws SQLException {
    this(new MySqlDataAccessor(dbEndpoints, localDatacenter, new MySqlMetrics(AccountStatsMySqlStore.class, registry)),
        clusterName, hostname, localBackupFilePath, hostnameHelper, registry);
  }

  /**
   * Constructor to create link {@link AccountStatsMySqlStore}. It's only used in tests.
   * @param dataAccessor The {@link MySqlDataAccessor}.
   * @param clusterName  The name of the cluster, like Ambry-test.
   * @param hostname The name of the host.
   * @param localBackupFilePath The filepath to local backup file.
   * @param hostnameHelper The {@link HostnameHelper} to simplify the hostname.
   * @param registry The {@link MetricRegistry}.
   */
  AccountStatsMySqlStore(MySqlDataAccessor dataAccessor, String clusterName, String hostname,
      String localBackupFilePath, HostnameHelper hostnameHelper, MetricRegistry registry) {
    mySqlDataAccessor = dataAccessor;
    accountReportsDao = new AccountReportsDao(dataAccessor, clusterName, hostname);
    aggregatedaccountReportsDao = new AggregatedAccountReportsDao(dataAccessor, clusterName);
    this.hostnameHelper = hostnameHelper;
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
   * Store the {@link StatsWrapper} to mysql database. This method ignores the error information from {@link StatsWrapper}
   * and only publish the container storage usages that are different from the previous one.
   * @param statsWrapper The {@link StatsWrapper} to publish.
   */
  @Override
  public void storeStats(StatsWrapper statsWrapper) {
    StatsSnapshot prevSnapshot =
        previousStats == null ? new StatsSnapshot((long) -1, new HashMap<>()) : previousStats.getSnapshot();
    applyFunctionToContainerUsageInDifferentStatsSnapshots(statsWrapper.getSnapshot(), prevSnapshot,
        this::tryUpdateStorageUsage);
    previousStats = statsWrapper;
  }

  /**
   * Query mysql database to get all the container storage usage for given {@code clusterName} and {@code hostname} and
   * construct a {@link StatsSnapshot} from them.
   * @param clusterName the clusterName.
   * @param hostname the hostname
   * @return {@link StatsSnapshot} published by the given host.
   * @throws SQLException
   */
  @Override
  public StatsWrapper queryStatsOf(String clusterName, String hostname) throws SQLException {
    hostname = hostnameHelper.simplifyHostname(hostname);
    Map<String, StatsSnapshot> partitionSubMap = new HashMap<>();
    StatsSnapshot hostSnapshot = new StatsSnapshot((long) 0, partitionSubMap);
    AtomicLong timestamp = new AtomicLong(0);
    accountReportsDao.queryStorageUsageForHost(clusterName, hostname,
        (partitionId, accountId, containerId, storageUsage, updatedAtMs) -> {
          StatsSnapshot partitionSnapshot = hostSnapshot.getSubMap()
              .computeIfAbsent("Partition[" + partitionId + "]", k -> new StatsSnapshot((long) 0, new HashMap<>()));
          StatsSnapshot accountSnapshot = partitionSnapshot.getSubMap()
              .computeIfAbsent("A[" + accountId + "]", k -> new StatsSnapshot((long) 0, new HashMap<>()));
          accountSnapshot.getSubMap().put("C[" + containerId + "]", new StatsSnapshot(storageUsage, null));
          timestamp.set(Math.max(timestamp.get(), updatedAtMs));
        });

    hostSnapshot.updateValue();
    return new StatsWrapper(
        new StatsHeader(StatsHeader.StatsDescription.STORED_DATA_SIZE, timestamp.get(), partitionSubMap.size(),
            partitionSubMap.size(), null), hostSnapshot);
  }

  /**
   * Store the aggregated account stats in {@link StatsSnapshot} to mysql database.
   * @param snapshot The aggregated account stats snapshot.
   */
  @Override
  public void storeAggregatedStats(StatsSnapshot snapshot) throws SQLException {
    int batchSize = 0;
    long startTimeMs = System.currentTimeMillis();
    for (Map.Entry<String, StatsSnapshot> accountMapEntry : snapshot.getSubMap().entrySet()) {
      String accountIdKey = accountMapEntry.getKey();
      short accountId = Short.valueOf(accountIdKey.substring(2, accountIdKey.length() - 1));
      StatsSnapshot containerStatsSnapshot = accountMapEntry.getValue();
      for (Map.Entry<String, StatsSnapshot> currContainerMapEntry : containerStatsSnapshot.getSubMap().entrySet()) {
        String containerIdKey = currContainerMapEntry.getKey();
        short containerId = Short.valueOf(containerIdKey.substring(2, containerIdKey.length() - 1));
        long currStorageUsage = currContainerMapEntry.getValue().getValue();
        aggregatedaccountReportsDao.updateStorageUsage(accountId, containerId, currStorageUsage);
        batchSize++;
      }
    }
    storeMetrics.aggregatedPublishTimeMs.update(System.currentTimeMillis() - startTimeMs);
    storeMetrics.aggregatedBatchSize.update(batchSize);
  }

  /**
   * Query mysql database to get all the aggregated container storage usage for given {@code clusterName} and construct
   * a map from those data. The map is structured as such:
   * <p>Outer map's key is the string format of account id, inner map's key is the string format of container id and the
   * value of the inner map is the storage usage of the container.</p>
   * @param clusterName the clusterName.
   * @return A map that represents container storage usage.
   * @throws Exception
   */
  @Override
  public Map<String, Map<String, Long>> queryAggregatedStats(String clusterName) throws Exception {
    Map<String, Map<String, Long>> result = new HashMap<>();
    aggregatedaccountReportsDao.queryContainerUsageForCluster(clusterName, (accountId, containerId, storageUsage) -> {
      result.computeIfAbsent(String.valueOf(accountId), k -> new HashMap<>())
          .put(String.valueOf(containerId), storageUsage);
    });
    return result;
  }

  @Override
  public Map<String, Map<String, Long>> queryMonthlyAggregatedStats(String clusterName) throws Exception {
    Map<String, Map<String, Long>> result = new HashMap<>();
    aggregatedaccountReportsDao.queryMonthlyContainerUsageForCluster(clusterName,
        (accountId, containerId, storageUsage) -> {
          result.computeIfAbsent(String.valueOf(accountId), k -> new HashMap<>())
              .put(String.valueOf(containerId), storageUsage);
        });
    return result;
  }

  @Override
  public String queryRecordedMonth(String clusterName) throws SQLException {
    return aggregatedaccountReportsDao.queryMonthForCluster(clusterName);
  }

  /**
   * Copy the row of table {@link AggregatedAccountReportsDao#AGGREGATED_ACCOUNT_REPORTS_TABLE} to {@link AggregatedAccountReportsDao#MONTHLY_AGGREGATED_ACCOUNT_REPORTS_TABLE}
   * and update the {@code monthValue} in table {@link AggregatedAccountReportsDao#AGGREGATED_ACCOUNT_REPORTS_MONTH_TABLE}.
   * @param clusterName The clusterName
   * @param monthValue The month value.
   * @throws Exception
   */
  @Override
  public void takeSnapshotOfAggregatedStatsAndUpdateMonth(String clusterName, String monthValue) throws Exception {
    aggregatedaccountReportsDao.copyAggregatedUsageToMonthlyAggregatedTableForCluster(clusterName);
    aggregatedaccountReportsDao.updateMonth(clusterName, monthValue);
  }

  /**
   * Return {@link #previousStats}. Only used in test.
   * @return
   */
  StatsWrapper getPreviousStats() {
    return previousStats;
  }

  /**
   * Return {@link #mySqlDataAccessor}. Only used in test.
   * @return
   */
  public MySqlDataAccessor getMySqlDataAccessor() {
    return mySqlDataAccessor;
  }

  /**
   * Find the differences between two {@link StatsSnapshot} and apply them to the given {@link ContainerUsageFunction}.
   * The difference is defined as
   * 1. If a container storage usage exists in both StatsSnapshot, and the values are different.
   * 2. If a container storage usage only exists in first StatsSnapshot.
   * If a container storage usage only exists in the second StatsSnapshot, then it will not be applied to the given function.
   * @param currentStats The current StatsSnapshot.
   * @param previousStats The previous StatsSnapshot.
   * @param func The function to apply the differences to.
   */
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
            func.apply(partitionId, accountId, containerId, currStorageUsage, System.currentTimeMillis());
            batchSize++;
          }
        }
      }
    }
    storeMetrics.publishTimeMs.update(System.currentTimeMillis() - startTimeMs);
    storeMetrics.batchSize.update(batchSize);
  }

  /**
   * Update storage usage with {@link AccountReportsDao} but ignore the exception.
   * @param partitionId The partition id of this account/container usage.
   * @param accountId The account id.
   * @param containerId The container id.
   * @param storageUsage The storage usage in bytes.
   * @param updatedAtMs The time in ms, will be ignored by {@link AccountReportsDao}.
   */
  private void tryUpdateStorageUsage(short partitionId, short accountId, short containerId, long storageUsage,
      long updatedAtMs) {
    try {
      accountReportsDao.updateStorageUsage(partitionId, accountId, containerId, storageUsage);
    } catch (Exception e) {
    }
  }
}
