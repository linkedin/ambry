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

  private final MySqlDataAccessor mySqlDataAccessor;
  private final AccountReportsDao accountReportsDao;
  private final HostnameHelper hostnameHelper;
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
   * Publish the {@link StatsWrapper} to mysql database. This method ignores the error information from {@link StatsWrapper}
   * and only publish the container storage usages that are different from the previous one.
   * @param statsWrapper The {@link StatsWrapper} to publish.
   */
  public void publish(StatsWrapper statsWrapper) {
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
  public StatsSnapshot queryStatsSnapshotOf(String clusterName, String hostname) throws SQLException {
    hostname = hostnameHelper.simplifyHostname(hostname);
    Map<String, StatsSnapshot> partitionSubMap = new HashMap<>();
    StatsSnapshot hostSnapshot = new StatsSnapshot((long) 0, partitionSubMap);
    accountReportsDao.queryStorageUsageForHost(clusterName, hostname,
        (partitionId, accountId, containerId, storageUsage) -> {
          StatsSnapshot partitionSnapshot = hostSnapshot.getSubMap()
              .computeIfAbsent("Partition[" + partitionId + "]", k -> new StatsSnapshot((long) 0, new HashMap<>()));
          StatsSnapshot accountSnapshot = partitionSnapshot.getSubMap()
              .computeIfAbsent("A[" + accountId + "]", k -> new StatsSnapshot((long) 0, new HashMap<>()));
          accountSnapshot.getSubMap().put("C[" + containerId + "]", new StatsSnapshot(storageUsage, null));
        });

    hostSnapshot.updateValue();
    return hostSnapshot;
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
            func.apply(partitionId, accountId, containerId, currStorageUsage);
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
   */
  private void tryUpdateStorageUsage(short partitionId, short accountId, short containerId, long storageUsage) {
    try {
      accountReportsDao.updateStorageUsage(partitionId, accountId, containerId, storageUsage);
    } catch (Exception e) {
    }
  }
}
