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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.config.AccountStatsMySqlConfig;
import com.github.ambry.mysql.MySqlDataAccessor;
import com.github.ambry.mysql.MySqlMetrics;
import com.github.ambry.mysql.MySqlUtils;
import com.github.ambry.server.AccountStatsStore;
import com.github.ambry.server.StatsHeader;
import com.github.ambry.server.StatsSnapshot;
import com.github.ambry.server.StatsWrapper;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import joptsimple.internal.Strings;


/**
 * This class publishes container storage usage to mysql. It saves previous copy of {@link StatsWrapper} and compare
 * the current {@link StatsWrapper} with the previous and only update the containers that have different storage usage.
 * It also assumes a local copy of {@link StatsWrapper} will be saved after publishing to mysql database, so it can recover
 * the previous {@link StatsWrapper} from crashing or restarting.
 */
public class AccountStatsMySqlStore implements AccountStatsStore {

  public static final String[] TABLES =
      {AccountReportsDao.ACCOUNT_REPORTS_TABLE, AggregatedAccountReportsDao.AGGREGATED_ACCOUNT_REPORTS_TABLE,
          AggregatedAccountReportsDao.AGGREGATED_ACCOUNT_REPORTS_MONTH_TABLE,
          AggregatedAccountReportsDao.MONTHLY_AGGREGATED_ACCOUNT_REPORTS_TABLE,
          PartitionClassReportsDao.PARTITION_CLASS_NAMES_TABLE, PartitionClassReportsDao.PARTITIONS_TABLE,
          PartitionClassReportsDao.AGGREGATED_PARTITION_CLASS_REPORTS_TABLE};

  private final MySqlDataAccessor mySqlDataAccessor;
  private final AccountReportsDao accountReportsDao;
  private final AggregatedAccountReportsDao aggregatedaccountReportsDao;
  private final PartitionClassReportsDao partitionClassReportsDao;
  private final HostnameHelper hostnameHelper;
  private final String clusterName;
  private final String hostname;
  private StatsWrapper previousStats;
  private final Metrics storeMetrics;
  private final AccountStatsMySqlConfig config;

  /**
   * Metrics for {@link AccountStatsMySqlStore}.
   */
  private static class Metrics {
    public final Histogram batchSize;
    public final Histogram publishTimeMs;
    public final Histogram aggregatedBatchSize;
    public final Histogram aggregatedPublishTimeMs;
    public final Histogram queryStatsTimeMs;
    public final Histogram queryAggregatedStatsTimeMs;
    public final Histogram queryMonthlyAggregatedStatsTimeMs;
    public final Histogram queryMonthTimeMs;
    public final Histogram takeSnapshotTimeMs;

    public final Histogram queryPartitionNameAndIdTimeMs;
    public final Histogram storePartitionClassStatsTimeMs;
    public final Histogram queryPartitionClassStatsTimeMs;
    public final Histogram storeAggregatedPartitionClassStatsTimeMs;
    public final Histogram queryAggregatedPartitionClassStatsTimeMs;

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
      queryStatsTimeMs = registry.histogram(MetricRegistry.name(AccountStatsMySqlStore.class, "QueryStatsTimeMs"));
      queryAggregatedStatsTimeMs =
          registry.histogram(MetricRegistry.name(AccountStatsMySqlStore.class, "QueryAggregatedStatsTimeMs"));
      queryMonthlyAggregatedStatsTimeMs =
          registry.histogram(MetricRegistry.name(AccountStatsMySqlStore.class, "QueryMonthlyAggregatedStatsTimeMs"));
      queryMonthTimeMs = registry.histogram(MetricRegistry.name(AccountStatsMySqlStore.class, "QueryMonthTimeMs"));
      takeSnapshotTimeMs = registry.histogram(MetricRegistry.name(AccountStatsMySqlStore.class, "TakeSnapshotTimeMs"));
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
    }
  }

  /**
   * Constructor to create {@link AccountStatsMySqlStore}.
   * @param config The {@link AccountStatsMySqlConfig}.
   * @param dbEndpoints MySql DB end points.
   * @param localDatacenter The local datacenter name. Endpoints from local datacenter are preferred when creating connection to MySql DB.
   * @param clusterName  The name of the cluster, like Ambry-test.
   * @param hostname The name of the host.
   * @param localBackupFilePath The filepath to local backup file.
   * @param hostnameHelper The {@link HostnameHelper} to simplify the hostname.
   * @param registry The {@link MetricRegistry}.
   * @throws SQLException
   */
  public AccountStatsMySqlStore(AccountStatsMySqlConfig config, List<MySqlUtils.DbEndpoint> dbEndpoints,
      String localDatacenter, String clusterName, String hostname, String localBackupFilePath,
      HostnameHelper hostnameHelper, MetricRegistry registry) throws SQLException {
    this(config,
        new MySqlDataAccessor(dbEndpoints, localDatacenter, new MySqlMetrics(AccountStatsMySqlStore.class, registry)),
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
  AccountStatsMySqlStore(AccountStatsMySqlConfig config, MySqlDataAccessor dataAccessor, String clusterName,
      String hostname, String localBackupFilePath, HostnameHelper hostnameHelper, MetricRegistry registry) {
    this.config = config;
    this.clusterName = clusterName;
    this.hostname = hostname;
    mySqlDataAccessor = dataAccessor;
    accountReportsDao = new AccountReportsDao(dataAccessor);
    aggregatedaccountReportsDao = new AggregatedAccountReportsDao(dataAccessor);
    partitionClassReportsDao = new PartitionClassReportsDao(dataAccessor);
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
  public synchronized void storeAccountStats(StatsWrapper statsWrapper) throws SQLException {
    StatsSnapshot prevSnapshot =
        previousStats == null ? new StatsSnapshot((long) -1, new HashMap<>()) : previousStats.getSnapshot();
    AccountReportsDao.StorageBatchUpdater batch = accountReportsDao.new StorageBatchUpdater(config.updateBatchSize);
    int batchSize = 0;
    long startTimeMs = System.currentTimeMillis();

    // Find the differences between two {@link StatsSnapshot} and apply them to the given {@link ContainerUsageFunction}.
    // The difference is defined as
    // 1. If a container storage usage exists in both StatsSnapshot, and the values are different.
    // 2. If a container storage usage only exists in first StatsSnapshot.
    // If a container storage usage only exists in the second StatsSnapshot, then it will not be applied to the given function.
    Map<String, StatsSnapshot> currPartitionMap = statsWrapper.getSnapshot().getSubMap();
    Map<String, StatsSnapshot> prevPartitionMap = prevSnapshot.getSubMap();
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
            batch.addUpdateToBatch(clusterName, hostname, partitionId, accountId, containerId, currStorageUsage);
            batchSize++;
          }
        }
      }
    }
    batch.flush();
    storeMetrics.publishTimeMs.update(System.currentTimeMillis() - startTimeMs);
    storeMetrics.batchSize.update(batchSize);
    previousStats = statsWrapper;
  }

  /**
   * Query mysql database to get all the container storage usage for given {@code clusterName} and {@code queryHostname} and
   * construct a {@link StatsSnapshot} from them.
   * @param queryHostname the hostname to query
   * @return {@link StatsSnapshot} published by the given host.
   * @throws SQLException
   */
  @Override
  public synchronized StatsWrapper queryAccountStatsOf(String queryHostname) throws SQLException {
    long startTimeMs = System.currentTimeMillis();
    queryHostname = hostnameHelper.simplifyHostname(queryHostname);
    Map<String, StatsSnapshot> partitionSubMap = new HashMap<>();
    StatsSnapshot hostSnapshot = new StatsSnapshot((long) 0, partitionSubMap);
    AtomicLong timestamp = new AtomicLong(0);
    accountReportsDao.queryStorageUsageForHost(clusterName, queryHostname,
        (partitionId, accountId, containerId, storageUsage, updatedAtMs) -> {
          StatsSnapshot partitionSnapshot = hostSnapshot.getSubMap()
              .computeIfAbsent("Partition[" + partitionId + "]", k -> new StatsSnapshot((long) 0, new HashMap<>()));
          StatsSnapshot accountSnapshot = partitionSnapshot.getSubMap()
              .computeIfAbsent("A[" + accountId + "]", k -> new StatsSnapshot((long) 0, new HashMap<>()));
          accountSnapshot.getSubMap().put("C[" + containerId + "]", new StatsSnapshot(storageUsage, null));
          timestamp.set(Math.max(timestamp.get(), updatedAtMs));
        });

    hostSnapshot.updateValue();
    storeMetrics.queryStatsTimeMs.update(System.currentTimeMillis() - startTimeMs);
    return new StatsWrapper(
        new StatsHeader(StatsHeader.StatsDescription.STORED_DATA_SIZE, timestamp.get(), partitionSubMap.size(),
            partitionSubMap.size(), null), hostSnapshot);
  }

  /**
   * Store the aggregated account stats in {@link StatsSnapshot} to mysql database.
   * @param snapshot The aggregated account stats snapshot.
   */
  @Override
  public synchronized void storeAggregatedAccountStats(StatsSnapshot snapshot) throws SQLException {
    int batchSize = 0;
    long startTimeMs = System.currentTimeMillis();
    AggregatedAccountReportsDao.AggregatedStorageBatchUpdater batch =
        aggregatedaccountReportsDao.new AggregatedStorageBatchUpdater(config.updateBatchSize);
    for (Map.Entry<String, StatsSnapshot> accountMapEntry : snapshot.getSubMap().entrySet()) {
      String accountIdKey = accountMapEntry.getKey();
      short accountId = Short.valueOf(accountIdKey.substring(2, accountIdKey.length() - 1));
      StatsSnapshot containerStatsSnapshot = accountMapEntry.getValue();
      for (Map.Entry<String, StatsSnapshot> currContainerMapEntry : containerStatsSnapshot.getSubMap().entrySet()) {
        String containerIdKey = currContainerMapEntry.getKey();
        short containerId = Short.valueOf(containerIdKey.substring(2, containerIdKey.length() - 1));
        long currStorageUsage = currContainerMapEntry.getValue().getValue();
        batch.addUpdateToBatch(clusterName, accountId, containerId, currStorageUsage);
        batchSize++;
      }
    }
    batch.flush();
    storeMetrics.aggregatedPublishTimeMs.update(System.currentTimeMillis() - startTimeMs);
    storeMetrics.aggregatedBatchSize.update(batchSize);
  }

  /**
   * Query mysql database to get all the aggregated container storage usage for given {@code clusterName} and construct
   * a map from those data. The map is structured as such:
   * <p>Outer map's key is the string format of account id, inner map's key is the string format of container id and the
   * value of the inner map is the storage usage of the container.</p>
   * @return A map that represents container storage usage.
   * @throws Exception
   */
  @Override
  public synchronized Map<String, Map<String, Long>> queryAggregatedAccountStats() throws Exception {
    long startTimeMs = System.currentTimeMillis();
    Map<String, Map<String, Long>> result = new HashMap<>();
    aggregatedaccountReportsDao.queryContainerUsageForCluster(clusterName, (accountId, containerId, storageUsage) -> {
      result.computeIfAbsent(String.valueOf(accountId), k -> new HashMap<>())
          .put(String.valueOf(containerId), storageUsage);
    });
    storeMetrics.queryAggregatedStatsTimeMs.update(System.currentTimeMillis() - startTimeMs);
    return result;
  }

  @Override
  public synchronized Map<String, Map<String, Long>> queryMonthlyAggregatedAccountStats() throws Exception {
    long startTimeMs = System.currentTimeMillis();
    Map<String, Map<String, Long>> result = new HashMap<>();
    aggregatedaccountReportsDao.queryMonthlyContainerUsageForCluster(clusterName,
        (accountId, containerId, storageUsage) -> {
          result.computeIfAbsent(String.valueOf(accountId), k -> new HashMap<>())
              .put(String.valueOf(containerId), storageUsage);
        });
    storeMetrics.queryMonthlyAggregatedStatsTimeMs.update(System.currentTimeMillis() - startTimeMs);
    return result;
  }

  @Override
  public synchronized String queryRecordedMonth() throws SQLException {
    long startTimeMs = System.currentTimeMillis();
    String result = aggregatedaccountReportsDao.queryMonthForCluster(clusterName);
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
  public synchronized void takeSnapshotOfAggregatedAccountStatsAndUpdateMonth(String monthValue) throws Exception {
    long startTimeMs = System.currentTimeMillis();
    aggregatedaccountReportsDao.copyAggregatedUsageToMonthlyAggregatedTableForCluster(clusterName);
    aggregatedaccountReportsDao.updateMonth(clusterName, monthValue);
    storeMetrics.takeSnapshotTimeMs.update(System.currentTimeMillis() - startTimeMs);
  }

  @Override
  public synchronized void storePartitionClassStats(StatsWrapper statsWrapper) throws SQLException {
    long startTimeMs = System.currentTimeMillis();
    // 1. Get all the partition class names in this statswrapper
    Map<String, StatsSnapshot> partitionClassSubMap = statsWrapper.getSnapshot().getSubMap();
    Set<String> partitionClassNames = new HashSet<>(partitionClassSubMap.keySet());
    Map<String, Short> partitionClassNamesInDB = partitionClassReportsDao.queryPartitionClassNames(clusterName);
    // 2. Add partition class names not in db
    partitionClassNames.removeAll(partitionClassNamesInDB.keySet());
    if (!partitionClassNames.isEmpty()) {
      for (String partitionClassName : partitionClassNames) {
        partitionClassReportsDao.insertPartitionClassName(clusterName, partitionClassName);
      }
      // Fresh the partition class names
      partitionClassNamesInDB = partitionClassReportsDao.queryPartitionClassNames(clusterName);
    }

    // 3. Get partition ids under each partition class name
    Map<String, List<Short>> partitionIdsUnderClassNames = partitionClassSubMap.entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, ent -> ent.getValue()
            .getSubMap()
            .keySet()
            .stream()
            .map(pk -> Short.valueOf(pk.substring("Partition[".length(), pk.length() - 1)))
            .collect(Collectors.toList())));

    // 4. Add partition ids not in db
    Set<Short> partitionIdsInDB = partitionClassReportsDao.queryPartitionIds(clusterName);
    for (String partitionClassName : partitionIdsUnderClassNames.keySet()) {
      short partitionClassId = partitionClassNamesInDB.get(partitionClassName);
      for (Short pid : partitionIdsUnderClassNames.get(partitionClassName)) {
        if (!partitionIdsInDB.contains(pid)) {
          partitionClassReportsDao.insertPartitionId(clusterName, pid, partitionClassId);
        }
      }
    }
    storeMetrics.storePartitionClassStatsTimeMs.update(System.currentTimeMillis() - startTimeMs);
  }

  @Override
  public synchronized void storeAggregatedPartitionClassStats(StatsSnapshot statsSnapshot) throws SQLException {
    long startTimeMs = System.currentTimeMillis();
    PartitionClassReportsDao.StorageBatchUpdater batch =
        partitionClassReportsDao.new StorageBatchUpdater(config.updateBatchSize);
    for (Map.Entry<String, StatsSnapshot> partitionClassMapEntry : statsSnapshot.getSubMap().entrySet()) {
      String partitionClassName = partitionClassMapEntry.getKey();
      for (Map.Entry<String, StatsSnapshot> accountContainerMapEntry : partitionClassMapEntry.getValue()
          .getSubMap()
          .entrySet()) {
        String accountContainer = accountContainerMapEntry.getKey();
        long usage = accountContainerMapEntry.getValue().getValue();
        String[] parts = accountContainer.split(Utils.ACCOUNT_CONTAINER_SEPARATOR);
        short accountId = Short.valueOf(parts[0].substring(2, parts[0].length() - 1));
        short containerId = Short.valueOf(parts[1].substring(2, parts[1].length() - 1));
        batch.addUpdateToBatch(clusterName, partitionClassName, accountId, containerId, usage);
      }
    }
    batch.flush();
    storeMetrics.storeAggregatedPartitionClassStatsTimeMs.update(System.currentTimeMillis() - startTimeMs);
  }

  @Override
  public synchronized StatsSnapshot queryAggregatedPartitionClassStatsOf() throws SQLException {
    long startTimeMs = System.currentTimeMillis();
    Map<String, Map<Short, Map<Short, Long>>> partitionClassNameAccountContainerUsages = new HashMap<>();
    AtomicLong timestamp = new AtomicLong(0);
    partitionClassReportsDao.queryAggregatedPartitionClassReport(clusterName,
        (partitionClassName, accountId, containerId, storageUsage, updatedAt) -> {
          partitionClassNameAccountContainerUsages.computeIfAbsent(partitionClassName, k -> new HashMap<>())
              .computeIfAbsent(accountId, k -> new HashMap<>())
              .put(containerId, storageUsage);
          timestamp.set(Math.max(timestamp.get(), updatedAt));
        });
    Map<String, StatsSnapshot> partitionClassNameSubMap = new HashMap<>();
    for (String partitionClassName : partitionClassNameAccountContainerUsages.keySet()) {
      Map<String, StatsSnapshot> accountContainerSubMap = new HashMap<>();
      for (short accountId : partitionClassNameAccountContainerUsages.get(partitionClassName).keySet()) {
        for (Map.Entry<Short, Long> usageEntry : partitionClassNameAccountContainerUsages.get(partitionClassName)
            .get(accountId)
            .entrySet()) {
          short containerId = usageEntry.getKey();
          long usage = usageEntry.getValue();
          accountContainerSubMap.put(
              "A[" + accountId + "]" + Utils.ACCOUNT_CONTAINER_SEPARATOR + "C[" + containerId + "]",
              new StatsSnapshot(usage, null));
        }
      }
      long accountContainerValue = accountContainerSubMap.values().stream().mapToLong(StatsSnapshot::getValue).sum();
      partitionClassNameSubMap.put(partitionClassName,
          new StatsSnapshot(accountContainerValue, accountContainerSubMap));
    }
    long partitionClassNameValue = partitionClassNameSubMap.values().stream().mapToLong(StatsSnapshot::getValue).sum();
    storeMetrics.queryAggregatedPartitionClassStatsTimeMs.update(System.currentTimeMillis() - startTimeMs);
    return new StatsSnapshot(partitionClassNameValue, partitionClassNameSubMap);
  }

  @Override
  public synchronized Map<String, Set<Short>> queryPartitionNameAndIds() throws SQLException {
    long startTimeMs = System.currentTimeMillis();
    Map<String, Set<Short>> result = partitionClassReportsDao.queryPartitionNameAndIds(clusterName);
    storeMetrics.queryPartitionNameAndIdTimeMs.update(System.currentTimeMillis() - startTimeMs);
    return result;
  }

  @Override
  public synchronized StatsWrapper queryPartitionClassStatsOf(String hostname,
      Map<String, Set<Short>> partitionNameAndIds) throws SQLException {
    long startTimeMs = System.currentTimeMillis();
    hostname = hostnameHelper.simplifyHostname(hostname);
    Map<Short, Map<Short, Map<Short, Long>>> partitionAccountContainerUsage = new HashMap<>();
    AtomicLong timestamp = new AtomicLong(0);
    accountReportsDao.queryStorageUsageForHost(clusterName, hostname,
        (partitionId, accountId, containerId, storageUsage, updatedAtMs) -> {
          partitionAccountContainerUsage.computeIfAbsent(partitionId, pid -> new HashMap<>())
              .computeIfAbsent(accountId, aid -> new HashMap<>())
              .put(containerId, storageUsage);
          timestamp.set(Math.max(timestamp.get(), updatedAtMs));
        });
    // Get all the partition ids;
    Set<Short> partitionIds = partitionAccountContainerUsage.keySet();
    Map<String, Set<Short>> partitionNameAndIdsForHost = new HashMap<>();
    for (Short partitionId : partitionIds) {
      boolean found = false;
      for (Map.Entry<String, Set<Short>> namesAndIdsEntry : partitionNameAndIds.entrySet()) {
        if (namesAndIdsEntry.getValue().contains(partitionId)) {
          partitionNameAndIdsForHost.computeIfAbsent(namesAndIdsEntry.getKey(), k -> new HashSet<>()).add(partitionId);
          found = true;
          break;
        }
      }
    }
    Map<String, StatsSnapshot> partitionClassSubMap = new HashMap<>();
    for (Map.Entry<String, Set<Short>> nameAndIdsEntry : partitionNameAndIdsForHost.entrySet()) {
      String partitionClassName = nameAndIdsEntry.getKey();
      Map<String, StatsSnapshot> partitionSubMap = new HashMap<>();
      for (short partitionId : nameAndIdsEntry.getValue()) {
        Map<Short, Map<Short, Long>> accountContainerUsage = partitionAccountContainerUsage.get(partitionId);
        Map<String, StatsSnapshot> accountContainerSubMap = new HashMap<>();
        for (short accountId : accountContainerUsage.keySet()) {
          Map<Short, Long> containerUsage = accountContainerUsage.get(accountId);
          containerUsage.entrySet()
              .forEach(ent -> accountContainerSubMap.put(
                  "A[" + accountId + "]" + Utils.ACCOUNT_CONTAINER_SEPARATOR + "C[" + ent.getKey() + "]",
                  new StatsSnapshot(ent.getValue(), null)));
        }
        long partitionValue = accountContainerSubMap.values().stream().mapToLong(StatsSnapshot::getValue).sum();
        StatsSnapshot partitionStats = new StatsSnapshot(partitionValue, accountContainerSubMap);
        partitionSubMap.put("Partition[" + partitionId + "]", partitionStats);
      }
      long partitionClassValue = partitionSubMap.values().stream().mapToLong(StatsSnapshot::getValue).sum();
      StatsSnapshot partitionClassStats = new StatsSnapshot(partitionClassValue, partitionSubMap);
      partitionClassSubMap.put(partitionClassName, partitionClassStats);
    }
    long hostValue = partitionClassSubMap.values().stream().mapToLong(StatsSnapshot::getValue).sum();
    StatsSnapshot hostStats = new StatsSnapshot(hostValue, partitionClassSubMap);

    storeMetrics.queryPartitionClassStatsTimeMs.update(System.currentTimeMillis() - startTimeMs);
    return new StatsWrapper(new StatsHeader(StatsHeader.StatsDescription.STORED_DATA_SIZE, timestamp.get(),
        partitionAccountContainerUsage.size(), partitionAccountContainerUsage.size(), null), hostStats);
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
   * Helper method to close the active connection, if there is one.
   */
  @Override
  public void closeConnection() {
    if (mySqlDataAccessor != null) {
      mySqlDataAccessor.closeActiveConnection();
    }
  }
}
