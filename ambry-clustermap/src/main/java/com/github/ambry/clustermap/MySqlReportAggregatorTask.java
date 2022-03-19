/**
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
package com.github.ambry.clustermap;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.github.ambry.accountstats.AccountStatsStore;
import com.github.ambry.commons.Callback;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.server.HostAccountStorageStatsWrapper;
import com.github.ambry.server.HostPartitionClassStorageStatsWrapper;
import com.github.ambry.server.StatsReportType;
import com.github.ambry.server.storagestats.AggregatedAccountStorageStats;
import com.github.ambry.server.storagestats.AggregatedPartitionClassStorageStats;
import com.github.ambry.server.storagestats.ContainerStorageStats;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.helix.HelixManager;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskResult;
import org.apache.helix.task.UserContentStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * MySql task to aggregate container storage usage across all storage nodes and update the aggregated stats.
 */
public class MySqlReportAggregatorTask extends UserContentStore implements Task {
  public static final String TASK_COMMAND_PREFIX = "mysql_aggregate";
  public static final ZoneOffset ZONE_OFFSET = ZoneId.systemDefault().getRules().getOffset(LocalDateTime.now());
  public static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM");
  private static final Logger logger = LoggerFactory.getLogger(MySqlReportAggregatorTask.class);
  private final MySqlClusterAggregator clusterAggregator;
  private final HelixManager manager;
  private final StatsReportType statsReportType;
  private final Callback<AggregatedAccountStorageStats> callback;
  private final ClusterMapConfig clusterMapConfig;
  private final AccountStatsStore accountStatsStore;
  private final Metrics metrics;
  private final Time time;

  /**
   * Metrics for {@link MySqlReportAggregatorTask}.
   */
  private static class Metrics {
    public final Histogram accountStatsFetchTimeMs;
    public final Histogram accountStatsAggregationTimeMs;
    public final Histogram partitionClassStatsFetchTimeMs;
    public final Histogram partitionClassStatsAggregationTimeMs;

    /**
     * Constructor to create the metrics;
     * @param registry
     */
    public Metrics(MetricRegistry registry) {
      accountStatsFetchTimeMs =
          registry.histogram(MetricRegistry.name(MySqlReportAggregatorTask.class, "AccountStatsFetchTimeMs"));
      accountStatsAggregationTimeMs =
          registry.histogram(MetricRegistry.name(MySqlReportAggregatorTask.class, "AccountStatsAggregationTimeMs"));
      partitionClassStatsFetchTimeMs =
          registry.histogram(MetricRegistry.name(MySqlReportAggregatorTask.class, "PartitionClassStatsFetchTimeMs"));
      partitionClassStatsAggregationTimeMs = registry.histogram(
          MetricRegistry.name(MySqlReportAggregatorTask.class, "PartitionClassStatsAggregationTimeMs"));
    }
  }

  /**
   * Instantiates {@link MySqlReportAggregatorTask}.
   * @param manager The {@link HelixManager} to retrieve all storage nodes.
   * @param relevantTimePeriodInMs relevant time period in ms within which values are considered to be valid. Values
   *                               outside of this period will be ignored.
   * @param statsReportType the type of stats report
   * @param accountStatsStore The {@link AccountStatsStore} to retrieve stats and store aggregated stats.
   * @param callback a callback which will be invoked when the aggregation report has been generated successfully.
   * @param clusterMapConfig the {@link ClusterMapConfig} associated with helix participant.
   * @param registry the {@link MetricRegistry}.
   */
  MySqlReportAggregatorTask(HelixManager manager, long relevantTimePeriodInMs, StatsReportType statsReportType,
      AccountStatsStore accountStatsStore, Callback<AggregatedAccountStorageStats> callback,
      ClusterMapConfig clusterMapConfig, MetricRegistry registry) {
    this.manager = manager;
    clusterAggregator = new MySqlClusterAggregator(relevantTimePeriodInMs);
    this.statsReportType = statsReportType;
    this.accountStatsStore = accountStatsStore;
    this.callback = callback;
    this.clusterMapConfig = clusterMapConfig;
    this.metrics = new Metrics(registry);
    this.time = SystemTime.getInstance();
  }

  @Override
  public TaskResult run() {
    Exception exception = null;
    Histogram fetchTimeMs = statsReportType == StatsReportType.ACCOUNT_REPORT ? metrics.accountStatsFetchTimeMs
        : metrics.partitionClassStatsFetchTimeMs;
    Histogram aggregationTimeMs =
        statsReportType == StatsReportType.ACCOUNT_REPORT ? metrics.accountStatsAggregationTimeMs
            : metrics.partitionClassStatsAggregationTimeMs;
    long startTimeMs = System.currentTimeMillis();
    AggregatedAccountStorageStats aggregatedAccountStorageStats = null;
    try {
      List<String> instanceNames = manager.getClusterManagmentTool().getInstancesInCluster(manager.getClusterName());
      if (statsReportType == StatsReportType.ACCOUNT_REPORT) {
        Map<String, HostAccountStorageStatsWrapper> accountStatsWrappers =
            fetchAccountStorageStatsWrapperForInstances(instanceNames);
        fetchTimeMs.update(System.currentTimeMillis() - startTimeMs);
        logger.info("Aggregating stats from " + accountStatsWrappers.size() + " hosts");
        Pair<AggregatedAccountStorageStats, AggregatedAccountStorageStats> results =
            clusterAggregator.aggregateHostAccountStorageStatsWrappers(accountStatsWrappers);
        if (clusterMapConfig.clustermapEnableDeleteInvalidDataInMysqlAggregationTask) {
          removeInvalidAggregatedAccountAndContainerStats(results.getSecond());
        }
        accountStatsStore.storeAggregatedAccountStorageStats(results.getSecond());
        aggregatedAccountStorageStats = results.getFirst();
      } else if (statsReportType == StatsReportType.PARTITION_CLASS_REPORT) {
        Map<String, HostPartitionClassStorageStatsWrapper> statsWrappers =
            fetchPartitionClassStorageStatsWrapperForInstances(instanceNames);
        fetchTimeMs.update(System.currentTimeMillis() - startTimeMs);
        logger.info("Aggregating stats from " + statsWrappers.size() + " hosts");
        Pair<AggregatedPartitionClassStorageStats, AggregatedPartitionClassStorageStats> results =
            clusterAggregator.aggregateHostPartitionClassStorageStatsWrappers(statsWrappers);
        if (clusterMapConfig.clustermapEnableDeleteInvalidDataInMysqlAggregationTask) {
          removeInvalidAggregatedPartitionClassStats(results.getSecond());
        }
        accountStatsStore.storeAggregatedPartitionClassStorageStats(results.getSecond());
      }

      // Create a base report at the beginning of each month.
      // Check if there is a base report for this month or not.
      if (clusterMapConfig.clustermapEnableAggregatedMonthlyAccountReport
          && statsReportType == StatsReportType.ACCOUNT_REPORT) {
        // Get the month, if not the same month, then copy the aggregated stats and update the month
        String currentMonthValue =
            LocalDateTime.ofEpochSecond(time.seconds(), 0, ZONE_OFFSET).format(TIMESTAMP_FORMATTER);
        String recordedMonthValue = accountStatsStore.queryRecordedMonth();
        if (recordedMonthValue == null || recordedMonthValue.isEmpty() || !currentMonthValue.equals(
            recordedMonthValue)) {
          if (clusterMapConfig.clustermapEnableDeleteInvalidDataInMysqlAggregationTask) {
            accountStatsStore.deleteSnapshotOfAggregatedAccountStats();
          }
          logger.info("Taking snapshot of aggregated stats for month " + currentMonthValue);
          accountStatsStore.takeSnapshotOfAggregatedAccountStatsAndUpdateMonth(currentMonthValue);
        }
      }
      aggregationTimeMs.update(System.currentTimeMillis() - startTimeMs);
      return new TaskResult(TaskResult.Status.COMPLETED, "Aggregation success");
    } catch (Exception e) {
      logger.error("Exception thrown while aggregating stats from container stats reports across all nodes ", e);
      exception = e;
      return new TaskResult(TaskResult.Status.FAILED, "Exception thrown");
    } finally {
      if (clusterMapConfig.clustermapEnableContainerDeletionAggregation && callback != null
          && aggregatedAccountStorageStats != null && statsReportType.equals(StatsReportType.ACCOUNT_REPORT)) {
        callback.onCompletion(aggregatedAccountStorageStats, exception);
      }
    }
  }

  private void removeInvalidAggregatedAccountAndContainerStats(AggregatedAccountStorageStats currentStats)
      throws Exception {
    AggregatedAccountStorageStats existingStats = accountStatsStore.queryAggregatedAccountStorageStats();
    List<Pair<Short, Short>> toBeDeletedAccountAndContainers = new ArrayList<>();
    for (Map.Entry<Short, Map<Short, ContainerStorageStats>> accountEntry : existingStats.getStorageStats()
        .entrySet()) {
      short accountId = accountEntry.getKey();
      for (short containerId : accountEntry.getValue().keySet()) {
        if (!currentStats.getStorageStats().containsKey(accountId) || !currentStats.getStorageStats()
            .get(accountId)
            .containsKey(containerId)) {
          toBeDeletedAccountAndContainers.add(new Pair<>(accountId, containerId));
        }
      }
    }
    // delete the account/container stats that are no longer valid
    for (Pair<Short, Short> accountContainer : toBeDeletedAccountAndContainers) {
      accountStatsStore.deleteAggregatedAccountStatsForContainer(accountContainer.getFirst(),
          accountContainer.getSecond());
    }
  }

  private void removeInvalidAggregatedPartitionClassStats(AggregatedPartitionClassStorageStats currentStats)
      throws Exception {
    List<Pair<String, Pair<Short, Short>>> toBeDeletedPartitionClassNameAndAccountContainer = new ArrayList<>();
    AggregatedPartitionClassStorageStats existingStats = accountStatsStore.queryAggregatedPartitionClassStorageStats();
    for (String partitionClassName : existingStats.getStorageStats().keySet()) {
      Map<Short, Map<Short, ContainerStorageStats>> currentAccountMap =
          currentStats.getStorageStats().get(partitionClassName);
      for (Map.Entry<Short, Map<Short, ContainerStorageStats>> accountEntry : existingStats.getStorageStats()
          .get(partitionClassName)
          .entrySet()) {
        short accountId = accountEntry.getKey();
        for (short containerId : accountEntry.getValue().keySet()) {
          if (currentAccountMap == null || !currentAccountMap.containsKey(accountId) || !currentAccountMap.get(
              accountId).containsKey(containerId)) {
            toBeDeletedPartitionClassNameAndAccountContainer.add(
                new Pair<>(partitionClassName, new Pair<>(accountId, containerId)));
          }
        }
      }
    }
    for (Pair<String, Pair<Short, Short>> pair : toBeDeletedPartitionClassNameAndAccountContainer) {
      Pair<Short, Short> accountContainerId = pair.getSecond();
      accountStatsStore.deleteAggregatedPartitionClassStatsForAccountContainer(pair.getFirst(),
          accountContainerId.getFirst(), accountContainerId.getSecond());
    }
  }

  /**
   * Fetch account storage stats report for each instance in {@code instanceNames}. Each instance name is probably a fully qualified
   * hostname with port number like this [hostname_portnumber]. It returns a map whose key is the instanceName and the value
   * is the {@link HostAccountStorageStatsWrapper} for each instance.
   * @param instanceNames The list of instance names to fetch account StatsWrapper.
   * @return A map of {@link HostAccountStorageStatsWrapper} for each instance name.
   * @throws Exception
   */
  private Map<String, HostAccountStorageStatsWrapper> fetchAccountStorageStatsWrapperForInstances(
      List<String> instanceNames) throws Exception {
    Map<String, HostAccountStorageStatsWrapper> statsWrappers = new HashMap<>();
    for (String instanceName : instanceNames) {
      Pair<String, Integer> pair = getHostNameAndPort(instanceName);
      statsWrappers.put(instanceName,
          accountStatsStore.queryHostAccountStorageStatsByHost(pair.getFirst(), pair.getSecond()));
    }
    return statsWrappers;
  }

  /**
   * Fetch partition class storage stats report for each instance in {@code instanceNames}. Each instance name is probably a fully qualified
   * hostname with port number like this [hostname_portnumber]. It returns a map whose key is the instanceName and the value
   * is the {@link HostPartitionClassStorageStatsWrapper} for each instance.
   * @param instanceNames The list of instance names to fetch partition class StatsWrapper.
   * @return A map of {@link HostPartitionClassStorageStatsWrapper} for each instance name.
   * @throws Exception
   */
  private Map<String, HostPartitionClassStorageStatsWrapper> fetchPartitionClassStorageStatsWrapperForInstances(
      List<String> instanceNames) throws Exception {
    Map<String, HostPartitionClassStorageStatsWrapper> statsWrappers = new HashMap<>();
    Map<String, Set<Integer>> partitionNameAndIds = accountStatsStore.queryPartitionNameAndIds();
    for (String instanceName : instanceNames) {
      Pair<String, Integer> pair = getHostNameAndPort(instanceName);
      statsWrappers.put(instanceName,
          accountStatsStore.queryHostPartitionClassStorageStatsByHost(pair.getFirst(), pair.getSecond(),
              partitionNameAndIds));
    }
    return statsWrappers;
  }

  private Pair<String, Integer> getHostNameAndPort(String instanceName) {
    String hostname = instanceName;
    int port = clusterMapConfig.clusterMapPort;
    int ind = instanceName.lastIndexOf("_");
    if (ind != -1) {
      try {
        port = Short.valueOf(instanceName.substring(ind + 1));
        hostname = instanceName.substring(0, ind);
      } catch (NumberFormatException e) {
        // String after "_" is not a port number, then the hostname should be the instanceName
      }
    }
    return new Pair<>(hostname, port);
  }

  @Override
  public void cancel() {

  }
}
