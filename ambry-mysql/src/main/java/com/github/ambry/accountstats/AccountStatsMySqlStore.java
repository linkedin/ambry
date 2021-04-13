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
import com.github.ambry.config.AccountStatsMySqlConfig;
import com.github.ambry.mysql.MySqlDataAccessor;
import com.github.ambry.mysql.MySqlMetrics;
import com.github.ambry.mysql.MySqlUtils;
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
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import joptsimple.internal.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class publishes container storage usage to mysql. It saves previous copy of {@link StatsWrapper} and compare
 * the current {@link StatsWrapper} with the previous and only update the containers that have different storage usage.
 * It also assumes a local copy of {@link StatsWrapper} will be saved after publishing to mysql database, so it can recover
 * the previous {@link StatsWrapper} from crashing or restarting.
 */
public class AccountStatsMySqlStore implements AccountStatsStore {
  private static final Logger logger = LoggerFactory.getLogger(AccountStatsMySqlStore.class);

  public static final String[] TABLES =
      {AccountReportsDao.ACCOUNT_REPORTS_TABLE, AggregatedAccountReportsDao.AGGREGATED_ACCOUNT_REPORTS_TABLE,
          AggregatedAccountReportsDao.AGGREGATED_ACCOUNT_REPORTS_MONTH_TABLE,
          AggregatedAccountReportsDao.MONTHLY_AGGREGATED_ACCOUNT_REPORTS_TABLE,
          PartitionClassReportsDao.PARTITION_CLASS_NAMES_TABLE, PartitionClassReportsDao.PARTITIONS_TABLE,
          PartitionClassReportsDao.AGGREGATED_PARTITION_CLASS_REPORTS_TABLE};

  private final MySqlDataAccessor mySqlDataAccessor;
  private final AccountReportsDao accountReportsDao;
  private final AggregatedAccountReportsDao aggregatedAccountReportsDao;
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

    public final Counter missingPartitionClassNameErrorCount;

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

      missingPartitionClassNameErrorCount =
          registry.counter(MetricRegistry.name(AccountStatsMySqlStore.class, "MissingPartitionClassNameErrorCount"));
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
    aggregatedAccountReportsDao = new AggregatedAccountReportsDao(dataAccessor);
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
  public void storeAccountStats(StatsWrapper statsWrapper) throws SQLException {
    StatsSnapshot prevSnapshot =
        previousStats == null ? new StatsSnapshot((long) -1, new HashMap<>()) : previousStats.getSnapshot();
    AccountReportsDao.StorageBatchUpdater batch = accountReportsDao.new StorageBatchUpdater(config.updateBatchSize);
    int batchSize = 0;
    long startTimeMs = System.currentTimeMillis();

    // Find the differences between two {@link StatsSnapshot} and apply them to the given {@link ContainerUsageFunction}.
    // The difference is defined as
    // 1. If a container storage usage exists in both StatsSnapshot, and the values are different.
    // 2. If a container storage usage only exists in current StatsSnapshot.
    // If a container storage usage only exists in the previous StatsSnapshot, then it will not be applied to the given function.
    // TODO: should delete rows in database when the previous statsSnapshot has more data than current one.
    Map<String, StatsSnapshot> currPartitionMap =
        Optional.ofNullable(statsWrapper.getSnapshot().getSubMap()).orElseGet(HashMap<String, StatsSnapshot>::new);
    Map<String, StatsSnapshot> prevPartitionMap =
        Optional.ofNullable(prevSnapshot.getSubMap()).orElseGet(HashMap<String, StatsSnapshot>::new);
    for (Map.Entry<String, StatsSnapshot> currPartitionMapEntry : currPartitionMap.entrySet()) {
      String partitionIdKey = currPartitionMapEntry.getKey();
      StatsSnapshot currAccountStatsSnapshot = currPartitionMapEntry.getValue();
      StatsSnapshot prevAccountStatsSnapshot =
          prevPartitionMap.getOrDefault(partitionIdKey, new StatsSnapshot((long) 0, new HashMap<>()));
      int partitionId = Utils.partitionIdFromStatsPartitionKey(partitionIdKey);
      // It's possible that this accountStatsSnapshot has empty submap, if all stats snapshots in the submap have 0
      // as its value.
      Map<String, StatsSnapshot> currAccountMap =
          Optional.ofNullable(currAccountStatsSnapshot.getSubMap()).orElseGet(HashMap<String, StatsSnapshot>::new);
      Map<String, StatsSnapshot> prevAccountMap =
          Optional.ofNullable(prevAccountStatsSnapshot.getSubMap()).orElseGet(HashMap<String, StatsSnapshot>::new);
      for (Map.Entry<String, StatsSnapshot> currAccountMapEntry : currAccountMap.entrySet()) {
        String accountIdKey = currAccountMapEntry.getKey();
        StatsSnapshot currContainerStatsSnapshot = currAccountMapEntry.getValue();
        StatsSnapshot prevContainerStatsSnapshot =
            prevAccountMap.getOrDefault(accountIdKey, new StatsSnapshot((long) 0, new HashMap<>()));
        short accountId = Utils.accountIdFromStatsAccountKey(accountIdKey);
        Map<String, StatsSnapshot> currContainerMap =
            Optional.ofNullable(currContainerStatsSnapshot.getSubMap()).orElseGet(HashMap<String, StatsSnapshot>::new);
        Map<String, StatsSnapshot> prevContainerMap =
            Optional.ofNullable(prevContainerStatsSnapshot.getSubMap()).orElseGet(HashMap<String, StatsSnapshot>::new);
        for (Map.Entry<String, StatsSnapshot> currContainerMapEntry : currContainerMap.entrySet()) {
          String containerIdKey = currContainerMapEntry.getKey();
          short containerId = Utils.containerIdFromStatsContainerKey(containerIdKey);
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
    storeMetrics.batchSize.update(batchSize);
    storeMetrics.publishTimeMs.update(System.currentTimeMillis() - startTimeMs);
    previousStats = statsWrapper;
  }

  /**
   * Query mysql database to get all the container storage usage for given {@code clusterName} and {@code queryHostname} and
   * construct a {@link StatsSnapshot} from them.
   * @param queryHostname the hostname to query
   * @param port the port number to query
   * @return {@link StatsSnapshot} published by the given host.
   * @throws SQLException
   */
  @Override
  public StatsWrapper queryAccountStatsByHost(String queryHostname, int port) throws SQLException {
    long startTimeMs = System.currentTimeMillis();
    queryHostname = hostnameHelper.simplifyHostname(queryHostname, port);
    Map<String, StatsSnapshot> partitionSubMap = new HashMap<>();
    StatsSnapshot hostSnapshot = new StatsSnapshot((long) 0, partitionSubMap);
    AtomicLong timestamp = new AtomicLong(0);
    accountReportsDao.queryStorageUsageForHost(clusterName, queryHostname,
        (partitionId, accountId, containerId, storageUsage, updatedAtMs) -> {
          StatsSnapshot partitionSnapshot = hostSnapshot.getSubMap()
              .computeIfAbsent(Utils.statsPartitionKey(partitionId), k -> new StatsSnapshot((long) 0, new HashMap<>()));
          StatsSnapshot accountSnapshot = partitionSnapshot.getSubMap()
              .computeIfAbsent(Utils.statsAccountKey(accountId), k -> new StatsSnapshot((long) 0, new HashMap<>()));
          accountSnapshot.getSubMap().put(Utils.statsContainerKey(containerId), new StatsSnapshot(storageUsage, null));
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
  public void storeAggregatedAccountStats(StatsSnapshot snapshot) throws SQLException {
    int batchSize = 0;
    long startTimeMs = System.currentTimeMillis();
    AggregatedAccountReportsDao.AggregatedStorageBatchUpdater batch =
        aggregatedAccountReportsDao.new AggregatedStorageBatchUpdater(config.updateBatchSize);
    for (Map.Entry<String, StatsSnapshot> accountMapEntry : snapshot.getSubMap().entrySet()) {
      String accountIdKey = accountMapEntry.getKey();
      short accountId = Utils.accountIdFromStatsAccountKey(accountIdKey);
      StatsSnapshot containerStatsSnapshot = accountMapEntry.getValue();
      for (Map.Entry<String, StatsSnapshot> currContainerMapEntry : containerStatsSnapshot.getSubMap().entrySet()) {
        String containerIdKey = currContainerMapEntry.getKey();
        short containerId = Utils.containerIdFromStatsContainerKey(containerIdKey);
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
  public Map<String, Map<String, Long>> queryAggregatedAccountStats() throws Exception {
    long startTimeMs = System.currentTimeMillis();
    Map<String, Map<String, Long>> result = new HashMap<>();
    aggregatedAccountReportsDao.queryContainerUsageForCluster(clusterName, (accountId, containerId, storageUsage) -> {
      result.computeIfAbsent(String.valueOf(accountId), k -> new HashMap<>())
          .put(String.valueOf(containerId), storageUsage);
    });
    storeMetrics.queryAggregatedStatsTimeMs.update(System.currentTimeMillis() - startTimeMs);
    return result;
  }

  @Override
  public StatsSnapshot queryAggregatedAccountStatsByClusterName(String clusterName) throws Exception {
    long startTimeMs = System.currentTimeMillis();
    Map<Short, Map<Short, Long>> accountContainerUsage = new HashMap<>();
    aggregatedAccountReportsDao.queryContainerUsageForCluster(clusterName, (accountId, containerId, storageUsage) -> {
      accountContainerUsage.computeIfAbsent(accountId, k -> new HashMap<>()).put(containerId, storageUsage);
    });
    if (accountContainerUsage.isEmpty()) {
      return null;
    }

    Map<String, StatsSnapshot> accountSubMap = new HashMap<>();
    for (Short accountId : accountContainerUsage.keySet()) {
      Map<String, StatsSnapshot> containerSubMap = accountContainerUsage.get(accountId)
          .entrySet()
          .stream()
          .collect(Collectors.toMap(ent -> Utils.statsContainerKey(ent.getKey()),
              ent -> new StatsSnapshot(ent.getValue(), null)));
      long containerValue = containerSubMap.values().stream().mapToLong(StatsSnapshot::getValue).sum();
      accountSubMap.put(Utils.statsAccountKey(accountId), new StatsSnapshot(containerValue, containerSubMap));
    }
    long accountValue = accountSubMap.values().stream().mapToLong(StatsSnapshot::getValue).sum();
    storeMetrics.queryAggregatedStatsTimeMs.update(System.currentTimeMillis() - startTimeMs);
    return new StatsSnapshot(accountValue, accountSubMap);
  }

  @Override
  public Map<String, Map<String, Long>> queryMonthlyAggregatedAccountStats() throws Exception {
    long startTimeMs = System.currentTimeMillis();
    Map<String, Map<String, Long>> result = new HashMap<>();
    aggregatedAccountReportsDao.queryMonthlyContainerUsageForCluster(clusterName,
        (accountId, containerId, storageUsage) -> {
          result.computeIfAbsent(String.valueOf(accountId), k -> new HashMap<>())
              .put(String.valueOf(containerId), storageUsage);
        });
    storeMetrics.queryMonthlyAggregatedStatsTimeMs.update(System.currentTimeMillis() - startTimeMs);
    return result;
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
  public void storePartitionClassStats(StatsWrapper statsWrapper) throws SQLException {
    long startTimeMs = System.currentTimeMillis();
    Map<String, StatsSnapshot> partitionClassSubMap = statsWrapper.getSnapshot().getSubMap();
    // 1. Get all the partition class names in this statswrapper and the partition class names in DB
    Set<String> partitionClassNames = new HashSet<>(partitionClassSubMap.keySet());
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
    Map<String, List<Integer>> partitionIdsUnderClassNames = partitionClassSubMap.entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, ent -> ent.getValue()
            .getSubMap()
            .keySet()
            .stream()
            .map(pk -> Utils.partitionIdFromStatsPartitionKey(pk))
            .collect(Collectors.toList())));

    // 4. Add partition ids not in DB
    Set<Integer> partitionIdsInDB = partitionClassReportsDao.queryPartitionIds(clusterName);
    for (String partitionClassName : partitionIdsUnderClassNames.keySet()) {
      short partitionClassId = partitionClassNamesInDB.get(partitionClassName);
      for (int pid : partitionIdsUnderClassNames.get(partitionClassName)) {
        if (!partitionIdsInDB.contains(pid)) {
          partitionClassReportsDao.insertPartitionId(clusterName, pid, partitionClassId);
        }
      }
    }
    storeMetrics.storePartitionClassStatsTimeMs.update(System.currentTimeMillis() - startTimeMs);
  }

  @Override
  public void storeAggregatedPartitionClassStats(StatsSnapshot statsSnapshot) throws SQLException {
    long startTimeMs = System.currentTimeMillis();
    PartitionClassReportsDao.StorageBatchUpdater batch =
        partitionClassReportsDao.new StorageBatchUpdater(config.updateBatchSize);
    // Aggregated partition class stats has two levels. The first level is the partion class name, the second level is the
    // accountId___containerId. It looks like this
    // {
    //  "default": {
    //    "A[1]___C[1]": { "v" : 1000 },
    //    "A[1]___C[2]": { "v" : 3000 },
    //    "A[2]___C[2]": { "v" : 3000 }
    //  },
    //  "new": {
    //    "A[1]___C[1]": { "v" : 1000 },
    //    "A[1]___C[2]": { "v" : 3000 },
    //    "A[2]___C[2]": { "v" : 3000 }
    //  }
    // }
    for (Map.Entry<String, StatsSnapshot> partitionClassMapEntry : statsSnapshot.getSubMap().entrySet()) {
      // Here, we have partition class names as the key of this entry and the StatsSnapshot containing the
      // accountId___ContiainerId in the subMap.
      String partitionClassName = partitionClassMapEntry.getKey();
      for (Map.Entry<String, StatsSnapshot> accountContainerMapEntry : partitionClassMapEntry.getValue()
          .getSubMap()
          .entrySet()) {
        // Here, we have the accountId___containerId as the key of this entry and the StatsSnapshot containing
        // the storage usage as the value.
        String accountContainer = accountContainerMapEntry.getKey();
        short[] accountContainerId = Utils.accountContainerIdFromPartitionClassStatsKey(accountContainer);
        long usage = accountContainerMapEntry.getValue().getValue();
        batch.addUpdateToBatch(clusterName, partitionClassName, accountContainerId[0], accountContainerId[1], usage);
      }
    }
    batch.flush();
    storeMetrics.storeAggregatedPartitionClassStatsTimeMs.update(System.currentTimeMillis() - startTimeMs);
  }

  @Override
  public StatsSnapshot queryAggregatedPartitionClassStats() throws SQLException {
    return queryAggregatedPartitionClassStatsByClusterName(this.clusterName);
  }

  @Override
  public StatsSnapshot queryAggregatedPartitionClassStatsByClusterName(String clusterName) throws SQLException {
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
    if (partitionClassNameAccountContainerUsages.isEmpty()) {
      return null;
    }
    // Here, partitionClassNameAccountContainerUsages map has partition class name, account id, container id as
    // keys of map at each level, the value is the storage usage.

    // Here we will construct a StatsSnapshot from partitionClassNameAccountContainerUsages.
    // The constructed StatsSnapshot would have two level of StatsSnapshot maps.
    // The first level is grouped by the partitionClassName.
    // The second level is grouped by the accounId___containerId.
    Map<String, StatsSnapshot> partitionClassNameSubMap = new HashMap<>();
    for (String partitionClassName : partitionClassNameAccountContainerUsages.keySet()) {
      Map<String, StatsSnapshot> accountContainerSubMap = new HashMap<>();
      for (short accountId : partitionClassNameAccountContainerUsages.get(partitionClassName).keySet()) {
        for (Map.Entry<Short, Long> usageEntry : partitionClassNameAccountContainerUsages.get(partitionClassName)
            .get(accountId)
            .entrySet()) {
          short containerId = usageEntry.getKey();
          long usage = usageEntry.getValue();
          accountContainerSubMap.put(Utils.partitionClassStatsAccountContainerKey(accountId, containerId),
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
  public Map<String, Set<Integer>> queryPartitionNameAndIds() throws SQLException {
    long startTimeMs = System.currentTimeMillis();
    Map<String, Set<Integer>> result = partitionClassReportsDao.queryPartitionNameAndIds(clusterName);
    storeMetrics.queryPartitionNameAndIdTimeMs.update(System.currentTimeMillis() - startTimeMs);
    return result;
  }

  @Override
  public StatsWrapper queryPartitionClassStatsByHost(String hostname, int port,
      Map<String, Set<Integer>> partitionNameAndIds) throws SQLException {
    long startTimeMs = System.currentTimeMillis();
    hostname = hostnameHelper.simplifyHostname(hostname, port);
    Map<Integer, Map<Short, Map<Short, Long>>> partitionAccountContainerUsage = new HashMap<>();
    AtomicLong timestamp = new AtomicLong(0);
    accountReportsDao.queryStorageUsageForHost(clusterName, hostname,
        (partitionId, accountId, containerId, storageUsage, updatedAtMs) -> {
          partitionAccountContainerUsage.computeIfAbsent(partitionId, pid -> new HashMap<>())
              .computeIfAbsent(accountId, aid -> new HashMap<>())
              .put(containerId, storageUsage);
          timestamp.set(Math.max(timestamp.get(), updatedAtMs));
        });
    // Here partitionAccountContainerUsage has partition id, account id and container id as keys of map at each level,
    // the value is the storage usage.

    // We have to construct a StatsSnapshot for the given host. Host-level StatsSnapshot is different than aggregated
    // StatsSnapshot. Aggregated StatsSnapshot only have two levels of map, but host-level StatsSnapshot has three levels
    // of map.
    // The first level is grouped by the partition class name just like aggregated StatsSnapshot.
    // The second level is grouped by the partition id that belongs to this partition class name.
    // The last level is grouped by the accountId___containerId.

    // As indicated by the comments above, we have to know the partition class name for each partition id before we
    // construct the StatsSnapshot. Luckily, we have all the partition ids and we have a map partitionNameAndIds whose
    // key is the partition class name and the value is the list of all partition ids belong to the partition class name.
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
    Map<String, StatsSnapshot> partitionClassSubMap = new HashMap<>();
    for (Map.Entry<String, Set<Integer>> nameAndIdsEntry : partitionNameAndIdsForHost.entrySet()) {
      String partitionClassName = nameAndIdsEntry.getKey();
      Map<String, StatsSnapshot> partitionSubMap = new HashMap<>();
      for (int partitionId : nameAndIdsEntry.getValue()) {
        Map<Short, Map<Short, Long>> accountContainerUsage = partitionAccountContainerUsage.get(partitionId);
        Map<String, StatsSnapshot> accountContainerSubMap = new HashMap<>();
        for (short accountId : accountContainerUsage.keySet()) {
          Map<Short, Long> containerUsage = accountContainerUsage.get(accountId);
          containerUsage.entrySet()
              .forEach(ent -> accountContainerSubMap.put(
                  Utils.partitionClassStatsAccountContainerKey(accountId, ent.getKey()),
                  new StatsSnapshot(ent.getValue(), null)));
        }
        long partitionValue = accountContainerSubMap.values().stream().mapToLong(StatsSnapshot::getValue).sum();
        StatsSnapshot partitionStats = new StatsSnapshot(partitionValue, accountContainerSubMap);
        partitionSubMap.put(Utils.statsPartitionKey(partitionId), partitionStats);
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
