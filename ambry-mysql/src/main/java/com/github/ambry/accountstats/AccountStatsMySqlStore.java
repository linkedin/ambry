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
package com.github.ambry.accountstats;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.github.ambry.config.AccountStatsMySqlConfig;
import com.github.ambry.mysql.MySqlMetrics;
import com.github.ambry.server.HostAccountStorageStatsWrapper;
import com.github.ambry.server.HostPartitionClassStorageStatsWrapper;
import com.github.ambry.server.StatsHeader;
import com.github.ambry.server.storagestats.AggregatedAccountStorageStats;
import com.github.ambry.server.storagestats.AggregatedPartitionClassStorageStats;
import com.github.ambry.server.storagestats.ContainerStorageStats;
import com.github.ambry.server.storagestats.HostAccountStorageStats;
import com.github.ambry.server.storagestats.HostPartitionClassStorageStats;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import joptsimple.internal.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class publishes container storage usage to mysql. It saves previous copy of {@link HostAccountStorageStatsWrapper} and compare
 * the current {@link HostAccountStorageStatsWrapper} with the previous and only update the containers that have different storage usage.
 * It also assumes a local copy of {@link HostAccountStorageStatsWrapper} will be saved after publishing to mysql database, so it can recover
 * the previous {@link HostAccountStorageStatsWrapper} from crashing or restarting.
 */
public class AccountStatsMySqlStore implements AccountStatsStore {
  private static final Logger logger = LoggerFactory.getLogger(AccountStatsMySqlStore.class);
  private static final ObjectMapper objectMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

  public static final String[] TABLES =
      {AccountReportsDao.ACCOUNT_REPORTS_TABLE, AggregatedAccountReportsDao.AGGREGATED_ACCOUNT_REPORTS_TABLE,
          AggregatedAccountReportsDao.AGGREGATED_ACCOUNT_REPORTS_MONTH_TABLE,
          AggregatedAccountReportsDao.MONTHLY_AGGREGATED_ACCOUNT_REPORTS_TABLE,
          PartitionClassReportsDao.PARTITION_CLASS_NAMES_TABLE, PartitionClassReportsDao.PARTITIONS_TABLE,
          PartitionClassReportsDao.AGGREGATED_PARTITION_CLASS_REPORTS_TABLE};

  private final DataSource dataSource;
  private final AccountReportsDao accountReportsDao;
  private final AggregatedAccountReportsDao aggregatedAccountReportsDao;
  private final PartitionClassReportsDao partitionClassReportsDao;
  private final HostnameHelper hostnameHelper;
  private final String clusterName;
  private final String hostname;
  private final Metrics storeMetrics;
  private final AccountStatsMySqlConfig config;
  private HostAccountStorageStatsWrapper previousHostAccountStorageStatsWrapper;

  /**
   * Metrics for {@link AccountStatsMySqlStore}.
   */
  private static class Metrics {
    public final Histogram batchSize;
    public final Histogram publishTimeMs;
    public final Histogram insertAccountStatsTimeMs;
    public final Histogram deleteAccountStatsTimeMs;
    public final Histogram deleteAccountStatsHostTimeMs;
    public final Histogram deleteStatementSize;
    public final Histogram aggregatedBatchSize;
    public final Histogram aggregatedPublishTimeMs;
    public final Histogram deleteAggregatedAccountStatsTimeMs;
    public final Histogram queryStatsTimeMs;
    public final Histogram queryAggregatedStatsTimeMs;
    public final Histogram queryMonthlyAggregatedStatsTimeMs;
    public final Histogram queryMonthTimeMs;
    public final Histogram takeSnapshotTimeMs;
    public final Histogram deleteSnapshotTimeMs;

    public final Histogram queryPartitionNameAndIdTimeMs;
    public final Histogram storePartitionClassStatsTimeMs;
    public final Histogram queryPartitionClassStatsTimeMs;
    public final Histogram storeAggregatedPartitionClassStatsTimeMs;
    public final Histogram queryAggregatedPartitionClassStatsTimeMs;
    public final Histogram deleteAggregatedPartitionClassStatsTimeMs;

    public final Counter missingPartitionClassNameErrorCount;

    /**
     * Constructor to create the Metrics.
     * @param registry The {@link MetricRegistry}.
     */
    public Metrics(MetricRegistry registry) {
      batchSize = registry.histogram(MetricRegistry.name(AccountStatsMySqlStore.class, "BatchSize"));
      publishTimeMs = registry.histogram(MetricRegistry.name(AccountStatsMySqlStore.class, "PublishTimeMs"));
      insertAccountStatsTimeMs =
          registry.histogram(MetricRegistry.name(AccountStatsMySqlStore.class, "InsertAccountStatsTimeMs"));
      deleteAccountStatsTimeMs =
          registry.histogram(MetricRegistry.name(AccountStatsMySqlStore.class, "DeleteAccountStatsTimeMs"));
      deleteAccountStatsHostTimeMs =
          registry.histogram(MetricRegistry.name(AccountStatsMySqlStore.class, "DeleteAccountStatsHostTimeMs"));
      deleteStatementSize =
          registry.histogram(MetricRegistry.name(AccountStatsMySqlStore.class, "DeleteStatementSize"));
      aggregatedBatchSize =
          registry.histogram(MetricRegistry.name(AccountStatsMySqlStore.class, "AggregatedBatchSize"));
      aggregatedPublishTimeMs =
          registry.histogram(MetricRegistry.name(AccountStatsMySqlStore.class, "AggregatedPublishTimeMs"));
      deleteAggregatedAccountStatsTimeMs =
          registry.histogram(MetricRegistry.name(AccountStatsMySqlStore.class, "DeleteAggregatedAccountStatsTimeMs"));
      queryStatsTimeMs = registry.histogram(MetricRegistry.name(AccountStatsMySqlStore.class, "QueryStatsTimeMs"));
      queryAggregatedStatsTimeMs =
          registry.histogram(MetricRegistry.name(AccountStatsMySqlStore.class, "QueryAggregatedStatsTimeMs"));
      queryMonthlyAggregatedStatsTimeMs =
          registry.histogram(MetricRegistry.name(AccountStatsMySqlStore.class, "QueryMonthlyAggregatedStatsTimeMs"));
      queryMonthTimeMs = registry.histogram(MetricRegistry.name(AccountStatsMySqlStore.class, "QueryMonthTimeMs"));
      takeSnapshotTimeMs = registry.histogram(MetricRegistry.name(AccountStatsMySqlStore.class, "TakeSnapshotTimeMs"));
      deleteSnapshotTimeMs =
          registry.histogram(MetricRegistry.name(AccountStatsMySqlStore.class, "DeleteSnapshotTimeMs"));
      queryPartitionNameAndIdTimeMs =
          registry.histogram(MetricRegistry.name(AccountStatsMySqlStore.class, "QueryPartitionNameAndIdsTimeMs"));
      storePartitionClassStatsTimeMs =
          registry.histogram(MetricRegistry.name(AccountStatsMySqlStore.class, "StorePartitionClassStatsTimeMs"));
      queryPartitionClassStatsTimeMs =
          registry.histogram(MetricRegistry.name(AccountStatsMySqlStore.class, "QueryPartitionClassStatsTimeMs"));
      storeAggregatedPartitionClassStatsTimeMs = registry.histogram(
          MetricRegistry.name(AccountStatsMySqlStore.class, "StoreAggregatedPartitionClassStatsTimeMs"));
      queryAggregatedPartitionClassStatsTimeMs = registry.histogram(
          MetricRegistry.name(AccountStatsMySqlStore.class, "QueryAggregatedPartitionClassStatsTimeMs"));
      deleteAggregatedPartitionClassStatsTimeMs = registry.histogram(
          MetricRegistry.name(AccountStatsMySqlStore.class, "DeleteAggregatedPartitionClassStatsTimeMs"));

      missingPartitionClassNameErrorCount =
          registry.counter(MetricRegistry.name(AccountStatsMySqlStore.class, "MissingPartitionClassNameErrorCount"));
    }
  }

  /**
   * Constructor to create link {@link AccountStatsMySqlStore}. It's only used in tests.
   * @param config The {@link AccountStatsMySqlConfig}.
   * @param dataSource The {@link DataSource}.
   * @param clusterName  The name of the cluster, like Ambry-test.
   * @param hostname The name of the host.
   * @param hostnameHelper The {@link HostnameHelper} to simplify the hostname.
   * @param registry The {@link MetricRegistry}.
   */
  public AccountStatsMySqlStore(AccountStatsMySqlConfig config, DataSource dataSource, String clusterName,
      String hostname, HostnameHelper hostnameHelper, MetricRegistry registry) {
    this.config = config;
    this.clusterName = clusterName;
    this.hostname = hostname;
    MySqlMetrics mySqlMetrics = new MySqlMetrics(AccountStatsMySqlStore.class, registry);
    this.dataSource = dataSource;
    accountReportsDao = new AccountReportsDao(dataSource, mySqlMetrics);
    aggregatedAccountReportsDao = new AggregatedAccountReportsDao(dataSource, mySqlMetrics);
    partitionClassReportsDao = new PartitionClassReportsDao(dataSource, mySqlMetrics);
    this.hostnameHelper = hostnameHelper;
    storeMetrics = new AccountStatsMySqlStore.Metrics(registry);
    try {
      this.previousHostAccountStorageStatsWrapper = queryHostAccountStorageStatsBySimplifiedHostName(hostname);
    } catch (Exception e) {
      logger.error("Failed to query account storage stats from mysql database for host : {}", hostname);
      //if query from mysql fails, try to get the previousStats from local backup file.
      readStatsFromLocalBackupFile();
    }
  }

  /**
   * Store the {@link HostAccountStorageStatsWrapper} to mysql database. This method ignores the error information from {@link HostAccountStorageStatsWrapper}
   * and only publish the container storage usages that are different from the previous one.
   * @param statsWrapper The {@link HostAccountStorageStatsWrapper} to publish.
   */
  @Override
  public void storeHostAccountStorageStats(HostAccountStorageStatsWrapper statsWrapper) throws Exception {
    AccountReportsDao.StorageBatchUpdater batch = accountReportsDao.new StorageBatchUpdater(config.updateBatchSize);
    int batchSize = 0;
    long startTimeMs = System.currentTimeMillis();
    HostAccountStorageStats prevHostStats = Optional.ofNullable(previousHostAccountStorageStatsWrapper)
        .map(HostAccountStorageStatsWrapper::getStats)
        .orElseGet(HostAccountStorageStats::new);
    HostAccountStorageStats currHostStats = statsWrapper.getStats();
    // Find the differences between two {@link HostAccountStorageStats} and apply them to the given {@link ContainerStorageStatsFunction}.
    // The difference is defined as
    // 1. If a container storage usage exists in both HostAccountStorageStats, and the values are different.
    // 2. If a container storage usage only exists in current HostAccountStorageStats.
    // If a container storage usage only exists in the previous HostAccountStorageStats, then it will not be applied to the given function.
    Map<Long, Map<Short, Map<Short, ContainerStorageStats>>> currPartitionMap = currHostStats.getStorageStats();
    Map<Long, Map<Short, Map<Short, ContainerStorageStats>>> prevPartitionMap = prevHostStats.getStorageStats();
    for (Map.Entry<Long, Map<Short, Map<Short, ContainerStorageStats>>> currPartitionMapEntry : currPartitionMap.entrySet()) {
      long partitionId = currPartitionMapEntry.getKey();
      Map<Short, Map<Short, ContainerStorageStats>> currAccountMap = currPartitionMapEntry.getValue();
      Map<Short, Map<Short, ContainerStorageStats>> prevAccountMap =
          prevPartitionMap.getOrDefault(partitionId, Collections.emptyMap());
      // It's possible that this accountStatsSnapshot has empty submap, if all stats snapshots in the submap have 0
      // as its value.
      for (Map.Entry<Short, Map<Short, ContainerStorageStats>> currAccountMapEntry : currAccountMap.entrySet()) {
        short accountId = currAccountMapEntry.getKey();
        Map<Short, ContainerStorageStats> currContainerMap = currAccountMapEntry.getValue();
        Map<Short, ContainerStorageStats> prevContainerMap =
            prevAccountMap.getOrDefault(accountId, Collections.emptyMap());
        for (Map.Entry<Short, ContainerStorageStats> currContainerMapEntry : currContainerMap.entrySet()) {
          short containerId = currContainerMapEntry.getKey();
          ContainerStorageStats currContainerStorageStats = currContainerMapEntry.getValue();
          if (!prevContainerMap.containsKey(containerId) || !currContainerStorageStats.equals(
              prevContainerMap.get(containerId))) {
            batch.addUpdateToBatch(clusterName, hostname, (int) partitionId, accountId, currContainerStorageStats);
            batchSize++;
          }
        }
      }
    }
    batch.flush();
    storeMetrics.batchSize.update(batchSize);
    storeMetrics.insertAccountStatsTimeMs.update(System.currentTimeMillis() - startTimeMs);

    deleteContainerAccountStats(prevPartitionMap, currPartitionMap);
    storeMetrics.publishTimeMs.update(System.currentTimeMillis() - startTimeMs);
    previousHostAccountStorageStatsWrapper = statsWrapper;
    writeStatsToLocalBackupFile();
  }

  @Override
  public void deleteHostAccountStorageStatsForHost(String hostname, int port) throws Exception {
    long startTimeMs = System.currentTimeMillis();
    hostname = hostnameHelper.simplifyHostname(hostname, port);
    accountReportsDao.deleteStorageUsageForHost(clusterName, hostname);
    storeMetrics.deleteAccountStatsHostTimeMs.update(System.currentTimeMillis() - startTimeMs);
  }

  /**
   * Read all the container storage usage from local backup file.
   */
  private void readStatsFromLocalBackupFile() {
    if (!Strings.isNullOrEmpty(config.localBackupFilePath)) {
      // load backup file and this backup is the previous stats
      ObjectMapper objectMapper = new ObjectMapper();
      try {
        this.previousHostAccountStorageStatsWrapper =
            objectMapper.readValue(new File(config.localBackupFilePath), HostAccountStorageStatsWrapper.class);
      } catch (Exception e) {
        this.previousHostAccountStorageStatsWrapper = null;
      }
    }
  }

  private void writeStatsToLocalBackupFile() throws IOException {
    File tempFile = new File(config.localBackupFilePath + ".tmp");
    File destFile = new File(config.localBackupFilePath);
    if (tempFile.createNewFile()) {
      objectMapper.writeValue(tempFile, previousHostAccountStorageStatsWrapper);
      if (!tempFile.renameTo(destFile)) {
        throw new IOException("Failed to rename " + tempFile.getAbsolutePath() + " to " + destFile.getAbsolutePath());
      }
    } else {
      throw new IOException("Temporary file creation failed when publishing stats " + tempFile.getAbsolutePath());
    }
  }

  /**
   * Delete container's accountstats data if partition, or account, or container exists in previous partition map but
   * missing in current partition map.
   * @param prevPartitionMap the previous partition stats map.
   * @param currPartitionMap the current partition stats map.
   * @throws SQLException
   */
  private void deleteContainerAccountStats(Map<Long, Map<Short, Map<Short, ContainerStorageStats>>> prevPartitionMap,
      Map<Long, Map<Short, Map<Short, ContainerStorageStats>>> currPartitionMap) throws SQLException {
    long deleteStartTimeMs = System.currentTimeMillis();
    int deleteStatementCounter = 0;
    // Now delete the rows that appear in previousStats but not in the currentStats
    for (Map.Entry<Long, Map<Short, Map<Short, ContainerStorageStats>>> prevPartitionMapEntry : prevPartitionMap.entrySet()) {
      long partitionId = prevPartitionMapEntry.getKey();
      Map<Short, Map<Short, ContainerStorageStats>> prevAccountMap = prevPartitionMapEntry.getValue();
      Map<Short, Map<Short, ContainerStorageStats>> currAccountMap = currPartitionMap.get(partitionId);
      if (prevAccountMap.size() != 0 && (currAccountMap == null || currAccountMap.size() == 0)) {
        accountReportsDao.deleteStorageUsageForPartition(clusterName, hostname, (int) partitionId);
        deleteStatementCounter++;
        continue;
      }
      if (prevAccountMap.size() != 0 && currAccountMap != null && currAccountMap.size() != 0) {
        for (Map.Entry<Short, Map<Short, ContainerStorageStats>> prevAccountMapEntry : prevAccountMap.entrySet()) {
          short accountId = prevAccountMapEntry.getKey();
          Map<Short, ContainerStorageStats> prevContainerMap = prevAccountMapEntry.getValue();
          Map<Short, ContainerStorageStats> currContainerMap = currAccountMap.get(accountId);
          if (prevContainerMap.size() != 0 && (currContainerMap == null || currContainerMap.size() == 0)) {
            accountReportsDao.deleteStorageUsageForAccount(clusterName, hostname, (int) partitionId, accountId);
            deleteStatementCounter++;
            continue;
          }
          if (prevContainerMap.size() != 0 && currContainerMap != null && currContainerMap.size() != 0) {
            Set<Short> containerIds = new HashSet<Short>(prevContainerMap.keySet());
            containerIds.removeAll(currContainerMap.keySet());
            for (short containerId : containerIds) {
              accountReportsDao.deleteStorageUsageForContainer(clusterName, hostname, (int) partitionId, accountId,
                  containerId);
              deleteStatementCounter++;
            }
          }
        }
      }
    }
    storeMetrics.deleteStatementSize.update(deleteStatementCounter);
    storeMetrics.deleteAccountStatsTimeMs.update(System.currentTimeMillis() - deleteStartTimeMs);
  }

  /**
   * Query mysql database to get all the container storage usage for given {@code queryHostname} and {@code port}
   * @param queryHostname the hostname to query
   * @param port the port number to query
   * @return {@link HostAccountStorageStatsWrapper} published by the given host.
   * @throws SQLException
   */
  @Override
  public HostAccountStorageStatsWrapper queryHostAccountStorageStatsByHost(String queryHostname, int port)
      throws SQLException {
    queryHostname = hostnameHelper.simplifyHostname(queryHostname, port);
    return queryHostAccountStorageStatsBySimplifiedHostName(queryHostname);
  }

  /**
   * Query mysql database to get all the container storage usage for given {@code clusterName} and {@code queryHostname} and
   * construct a {@link HostAccountStorageStatsWrapper} from them
   * @param queryHostname The simplified hostname to query.
   * @return {@link HostAccountStorageStatsWrapper} published by the given simplified hostname.
   * @throws SQLException
   */
  private HostAccountStorageStatsWrapper queryHostAccountStorageStatsBySimplifiedHostName(String queryHostname)
      throws SQLException {
    long startTimeMs = System.currentTimeMillis();
    HostAccountStorageStats hostAccountStorageStats = new HostAccountStorageStats();
    AtomicLong timestamp = new AtomicLong(0);
    accountReportsDao.queryStorageUsageForHost(clusterName, queryHostname,
        (partitionId, accountId, containerStats, updatedAtMs) -> {
          hostAccountStorageStats.addContainerStorageStats(partitionId, accountId, containerStats);
          timestamp.set(Math.max(timestamp.get(), updatedAtMs));
        });

    storeMetrics.queryStatsTimeMs.update(System.currentTimeMillis() - startTimeMs);
    return new HostAccountStorageStatsWrapper(
        new StatsHeader(StatsHeader.StatsDescription.STORED_DATA_SIZE, timestamp.get(),
            hostAccountStorageStats.getStorageStats().size(), hostAccountStorageStats.getStorageStats().size(), null),
        hostAccountStorageStats);
  }

  /**
   * Store the {@link AggregatedAccountStorageStats} to mysql database.
   * @param aggregatedAccountStorageStats The {@link AggregatedAccountStorageStats}.
   */
  @Override
  public void storeAggregatedAccountStorageStats(AggregatedAccountStorageStats aggregatedAccountStorageStats)
      throws SQLException {
    int batchSize = 0;
    long startTimeMs = System.currentTimeMillis();
    AggregatedAccountReportsDao.AggregatedStorageBatchUpdater batch =
        aggregatedAccountReportsDao.new AggregatedStorageBatchUpdater(config.updateBatchSize);
    for (Map.Entry<Short, Map<Short, ContainerStorageStats>> accountMapEntry : aggregatedAccountStorageStats.getStorageStats()
        .entrySet()) {
      short accountId = accountMapEntry.getKey();
      Map<Short, ContainerStorageStats> containerMap = accountMapEntry.getValue();
      for (Map.Entry<Short, ContainerStorageStats> containerMapEntry : containerMap.entrySet()) {
        batch.addUpdateToBatch(clusterName, accountId, containerMapEntry.getValue());
        batchSize++;
      }
    }
    batch.flush();
    storeMetrics.aggregatedPublishTimeMs.update(System.currentTimeMillis() - startTimeMs);
    storeMetrics.aggregatedBatchSize.update(batchSize);
  }

  @Override
  public void deleteAggregatedAccountStatsForContainer(short accountId, short containerId) throws SQLException {
    long startTimeMs = System.currentTimeMillis();
    aggregatedAccountReportsDao.deleteStorageUsage(clusterName, accountId, containerId);
    storeMetrics.deleteAggregatedAccountStatsTimeMs.update(System.currentTimeMillis() - startTimeMs);
  }

  @Override
  public AggregatedAccountStorageStats queryAggregatedAccountStorageStats() throws Exception {
    return queryAggregatedAccountStorageStatsByClusterName(clusterName);
  }

  @Override
  public AggregatedAccountStorageStats queryAggregatedAccountStorageStatsByClusterName(String clusterName)
      throws Exception {
    long startTimeMs = System.currentTimeMillis();
    AggregatedAccountStorageStats aggregatedAccountStorageStats = new AggregatedAccountStorageStats(null);
    aggregatedAccountReportsDao.queryContainerUsageForCluster(clusterName, (accountId, containerStats) -> {
      aggregatedAccountStorageStats.addContainerStorageStats(accountId, containerStats);
    });
    storeMetrics.queryAggregatedStatsTimeMs.update(System.currentTimeMillis() - startTimeMs);
    return aggregatedAccountStorageStats;
  }

  @Override
  public AggregatedAccountStorageStats queryMonthlyAggregatedAccountStorageStats() throws Exception {
    long startTimeMs = System.currentTimeMillis();
    AggregatedAccountStorageStats aggregatedAccountStorageStats = new AggregatedAccountStorageStats(null);
    aggregatedAccountReportsDao.queryMonthlyContainerUsageForCluster(clusterName, (accountId, containerStats) -> {
      aggregatedAccountStorageStats.addContainerStorageStats(accountId, containerStats);
    });
    storeMetrics.queryMonthlyAggregatedStatsTimeMs.update(System.currentTimeMillis() - startTimeMs);
    return aggregatedAccountStorageStats;
  }

  @Override
  public String queryRecordedMonth() throws SQLException {
    long startTimeMs = System.currentTimeMillis();
    String result = aggregatedAccountReportsDao.queryMonthForCluster(clusterName);
    storeMetrics.queryMonthTimeMs.update(System.currentTimeMillis() - startTimeMs);
    return result;
  }

  /**
   * Copy the row of table {@link AggregatedAccountReportsDao#AGGREGATED_ACCOUNT_REPORTS_TABLE} to {@link AggregatedAccountReportsDao#MONTHLY_AGGREGATED_ACCOUNT_REPORTS_TABLE}
   * and update the {@code monthValue} in table {@link AggregatedAccountReportsDao#AGGREGATED_ACCOUNT_REPORTS_MONTH_TABLE}.
   * @param monthValue The month value.
   * @throws Exception
   */
  @Override
  public void takeSnapshotOfAggregatedAccountStatsAndUpdateMonth(String monthValue) throws Exception {
    long startTimeMs = System.currentTimeMillis();
    aggregatedAccountReportsDao.copyAggregatedUsageToMonthlyAggregatedTableForCluster(clusterName);
    aggregatedAccountReportsDao.updateMonth(clusterName, monthValue);
    storeMetrics.takeSnapshotTimeMs.update(System.currentTimeMillis() - startTimeMs);
  }

  @Override
  public void deleteSnapshotOfAggregatedAccountStats() throws SQLException {
    long startTimeMs = System.currentTimeMillis();
    aggregatedAccountReportsDao.deleteAggregatedUsageFromMonthlyAggregatedTableForCluster(clusterName);
    storeMetrics.deleteSnapshotTimeMs.update(System.currentTimeMillis() - startTimeMs);
  }

  @Override
  public void storeHostPartitionClassStorageStats(HostPartitionClassStorageStatsWrapper statsWrapper)
      throws SQLException {
    long startTimeMs = System.currentTimeMillis();
    Map<String, Map<Long, Map<Short, Map<Short, ContainerStorageStats>>>> partitionClassStorageStats =
        statsWrapper.getStats().getStorageStats();
    // 1. Get all the partition class names in this statswrapper and the partition class names in DB
    Set<String> partitionClassNames = new HashSet<>(partitionClassStorageStats.keySet());
    Map<String, Short> partitionClassNamesInDB = partitionClassReportsDao.queryPartitionClassNames(clusterName);

    // 2. Add partition class names not in DB
    partitionClassNames.removeAll(partitionClassNamesInDB.keySet());
    if (!partitionClassNames.isEmpty()) {
      for (String partitionClassName : partitionClassNames) {
        partitionClassReportsDao.insertPartitionClassName(clusterName, partitionClassName);
      }
      // Refresh the partition class names
      partitionClassNamesInDB = partitionClassReportsDao.queryPartitionClassNames(clusterName);
    }

    // 3. Get partition ids under each partition class name
    // The result looks like
    //  {
    //    "default": [1, 2, 3],
    //    "new": [4, 5, 6]
    //  }
    // The "default" and "new" are the partition class names and the numbers are partition ids. Same partition
    // can't belong to different partition class names.
    Map<String, List<Long>> partitionIdsUnderClassNames = partitionClassStorageStats.entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, ent -> new ArrayList<>(ent.getValue().keySet())));

    // 4. Add partition ids not in DB
    Set<Integer> partitionIdsInDB = partitionClassReportsDao.queryPartitionIds(clusterName);
    for (String partitionClassName : partitionIdsUnderClassNames.keySet()) {
      short partitionClassId = partitionClassNamesInDB.get(partitionClassName);
      for (long pid : partitionIdsUnderClassNames.get(partitionClassName)) {
        if (!partitionIdsInDB.contains(pid)) {
          partitionClassReportsDao.insertPartitionId(clusterName, (int) pid, partitionClassId);
        }
      }
    }
    storeMetrics.storePartitionClassStatsTimeMs.update(System.currentTimeMillis() - startTimeMs);
  }

  @Override
  public void storeAggregatedPartitionClassStorageStats(
      AggregatedPartitionClassStorageStats aggregatedPartitionClassStorageStats) throws SQLException {
    long startTimeMs = System.currentTimeMillis();
    PartitionClassReportsDao.StorageBatchUpdater batch =
        partitionClassReportsDao.new StorageBatchUpdater(config.updateBatchSize);
    Map<String, Map<Short, Map<Short, ContainerStorageStats>>> storageStats =
        aggregatedPartitionClassStorageStats.getStorageStats();
    for (String partitionClassName : storageStats.keySet()) {
      Map<Short, Map<Short, ContainerStorageStats>> accountStorageStats = storageStats.get(partitionClassName);
      for (short accountId : accountStorageStats.keySet()) {
        for (ContainerStorageStats containerStats : accountStorageStats.get(accountId).values()) {
          batch.addUpdateToBatch(clusterName, partitionClassName, accountId, containerStats);
        }
      }
    }
    batch.flush();
    storeMetrics.storeAggregatedPartitionClassStatsTimeMs.update(System.currentTimeMillis() - startTimeMs);
  }

  @Override
  public void deleteAggregatedPartitionClassStatsForAccountContainer(String partitionClassName, short accountId,
      short containerId) throws SQLException {
    long startTimeMs = System.currentTimeMillis();
    partitionClassReportsDao.deleteAggregatedStorageUsage(clusterName, partitionClassName, accountId, containerId);
    storeMetrics.deleteAggregatedPartitionClassStatsTimeMs.update(System.currentTimeMillis() - startTimeMs);
  }

  @Override
  public AggregatedPartitionClassStorageStats queryAggregatedPartitionClassStorageStats() throws SQLException {
    return queryAggregatedPartitionClassStorageStatsByClusterName(clusterName);
  }

  @Override
  public AggregatedPartitionClassStorageStats queryAggregatedPartitionClassStorageStatsByClusterName(String clusterName)
      throws SQLException {
    long startTimeMs = System.currentTimeMillis();
    AggregatedPartitionClassStorageStats aggregatedPartitionClassStorageStats =
        new AggregatedPartitionClassStorageStats(null);
    AtomicLong timestamp = new AtomicLong(0);
    partitionClassReportsDao.queryAggregatedPartitionClassReport(clusterName,
        (partitionClassName, accountId, containerStats, updatedAt) -> {
          aggregatedPartitionClassStorageStats.addContainerStorageStats(partitionClassName, accountId, containerStats);
          timestamp.set(Math.max(timestamp.get(), updatedAt));
        });
    storeMetrics.queryAggregatedPartitionClassStatsTimeMs.update(System.currentTimeMillis() - startTimeMs);
    return aggregatedPartitionClassStorageStats;
  }

  @Override
  public void shutdown() {
    if (dataSource instanceof AutoCloseable) {
      try {
        ((AutoCloseable) dataSource).close();
      } catch (Exception e) {
        logger.error("Failed to close data source: ", e);
      }
    }
  }

  @Override
  public Map<String, Set<Integer>> queryPartitionNameAndIds() throws SQLException {
    long startTimeMs = System.currentTimeMillis();
    Map<String, Set<Integer>> result = partitionClassReportsDao.queryPartitionNameAndIds(clusterName);
    storeMetrics.queryPartitionNameAndIdTimeMs.update(System.currentTimeMillis() - startTimeMs);
    return result;
  }

  @Override
  public HostPartitionClassStorageStatsWrapper queryHostPartitionClassStorageStatsByHost(String hostname, int port,
      Map<String, Set<Integer>> partitionNameAndIds) throws SQLException {
    long startTimeMs = System.currentTimeMillis();
    hostname = hostnameHelper.simplifyHostname(hostname, port);
    Map<Integer, Map<Short, Map<Short, ContainerStorageStats>>> partitionAccountContainerUsage = new HashMap<>();
    AtomicLong timestamp = new AtomicLong(0);
    accountReportsDao.queryStorageUsageForHost(clusterName, hostname,
        (partitionId, accountId, containerStats, updatedAtMs) -> {
          partitionAccountContainerUsage.computeIfAbsent(partitionId, pid -> new HashMap<>())
              .computeIfAbsent(accountId, aid -> new HashMap<>())
              .put(containerStats.getContainerId(), containerStats);
          timestamp.set(Math.max(timestamp.get(), updatedAtMs));
        });
    // Here partitionAccountContainerUsage has partition id, account id and container id as keys of map at each level,
    // the value is the storage usage.

    // As indicated by the comments above, we have to know the partition class name for each partition id. Luckily, we
    // have all the partition ids and we have a map partitionNameAndIds whose key is the partition class name and the
    //value is the list of all partition ids belong to the partition class name.
    Set<Integer> partitionIds = partitionAccountContainerUsage.keySet();
    Map<String, Set<Integer>> partitionNameAndIdsForHost = new HashMap<>();
    for (int partitionId : partitionIds) {
      boolean found = false;
      for (Map.Entry<String, Set<Integer>> namesAndIdsEntry : partitionNameAndIds.entrySet()) {
        if (namesAndIdsEntry.getValue().contains(partitionId)) {
          partitionNameAndIdsForHost.computeIfAbsent(namesAndIdsEntry.getKey(), k -> new HashSet<>()).add(partitionId);
          found = true;
          break;
        }
      }
      if (!found) {
        storeMetrics.missingPartitionClassNameErrorCount.inc();
        logger.error("Can't find partition class name for partition id {}", partitionId);
      }
    }
    HostPartitionClassStorageStats hostPartitionClassStorageStats = new HostPartitionClassStorageStats();
    for (Map.Entry<String, Set<Integer>> nameAndIdsEntry : partitionNameAndIdsForHost.entrySet()) {
      String partitionClassName = nameAndIdsEntry.getKey();
      for (int partitionId : nameAndIdsEntry.getValue()) {
        Map<Short, Map<Short, ContainerStorageStats>> accountContainerUsage =
            partitionAccountContainerUsage.get(partitionId);
        for (short accountId : accountContainerUsage.keySet()) {
          Map<Short, ContainerStorageStats> containerUsage = accountContainerUsage.get(accountId);
          containerUsage.values()
              .stream()
              .forEach(containerStats -> hostPartitionClassStorageStats.addContainerStorageStats(partitionClassName,
                  partitionId, accountId, containerStats));
        }
      }
    }

    storeMetrics.queryPartitionClassStatsTimeMs.update(System.currentTimeMillis() - startTimeMs);
    return new HostPartitionClassStorageStatsWrapper(
        new StatsHeader(StatsHeader.StatsDescription.STORED_DATA_SIZE, timestamp.get(),
            partitionAccountContainerUsage.size(), partitionAccountContainerUsage.size(), null),
        hostPartitionClassStorageStats);
  }

  /**
   * Remove all the data in the all account stats related tables; This is only used in test.
   */
  public void cleanupTables() throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      try (Statement statement = connection.createStatement()) {
        for (String table : AccountStatsMySqlStore.TABLES) {
          statement.executeUpdate("DELETE FROM " + table);
        }
      }
    }
  }

  /**
   * Return {@link #previousHostAccountStorageStatsWrapper}. Only used in test.
   * @return
   */
  HostAccountStorageStatsWrapper getPreviousHostAccountStorageStatsWrapper() {
    return previousHostAccountStorageStatsWrapper;
  }

  /**
   * Only used in test. Close connection after finishing using it.
   * @return A {@link Connection}.
   * @throws SQLException
   */
  Connection getConnection() throws SQLException {
    return dataSource.getConnection();
  }

  /**
   * Only used in test
   * @return The {@link DataSource}.
   */
  DataSource getDataSource() {
    return dataSource;
  }
}
