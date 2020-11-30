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

import com.github.ambry.commons.Callback;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.server.AccountStatsStore;
import com.github.ambry.server.StatsReportType;
import com.github.ambry.server.StatsSnapshot;
import com.github.ambry.server.StatsWrapper;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
  private static final Logger logger = LoggerFactory.getLogger(MySqlReportAggregatorTask.class);
  private static final ZoneOffset zoneOffset = ZoneId.systemDefault().getRules().getOffset(LocalDateTime.now());
  static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM");
  private final HelixClusterAggregator clusterAggregator;
  private final HelixManager manager;
  private final StatsReportType statsReportType;
  private final Callback<StatsSnapshot> callback;
  private final ClusterMapConfig clusterMapConfig;
  private final AccountStatsStore accountStatsStore;
  private final Time time;

  /**
   * Instantiates {@link MySqlReportAggregatorTask}.
   * @param manager The {@link HelixManager} to retrieve all storage nodes.
   * @param relevantTimePeriodInMs relevant time period in ms within which values are considered to be valid. Values
   *                               outside of this period will be ignored.
   * @param statsReportType the type of stats report
   * @param accountStatsStore The {@link AccountStatsStore} to retrieve stats and store aggregated stats.
   * @param callback a callback which will be invoked when the aggregation report has been generated successfully.
   * @param clusterMapConfig the {@link ClusterMapConfig} associated with helix participant.
   */
  MySqlReportAggregatorTask(HelixManager manager, long relevantTimePeriodInMs, StatsReportType statsReportType,
      AccountStatsStore accountStatsStore, Callback<StatsSnapshot> callback, ClusterMapConfig clusterMapConfig) {
    this.manager = manager;
    clusterAggregator = new HelixClusterAggregator(relevantTimePeriodInMs);
    this.statsReportType = statsReportType;
    this.accountStatsStore = accountStatsStore;
    this.callback = callback;
    this.clusterMapConfig = clusterMapConfig;
    this.time = SystemTime.getInstance();
  }

  @Override
  public TaskResult run() {
    Pair<StatsSnapshot, StatsSnapshot> results = null;
    Exception exception = null;
    try {
      List<String> instanceNames = manager.getClusterManagmentTool().getInstancesInCluster(manager.getClusterName());
      Map<String, StatsWrapper> statsWrappers = new HashMap<>();
      for (String instanceName : instanceNames) {
        // Helix instance name would suffix port number, here let's take port number out.
        instanceName = stripPortNumber(instanceName);
        statsWrappers.put(instanceName,
            accountStatsStore.queryStatsOf(clusterMapConfig.clusterMapClusterName, instanceName));
      }
      results = clusterAggregator.doWorkOnStatsWrapperMap(statsWrappers, statsReportType);
      accountStatsStore.storeAggregatedStats(results.getSecond());
      // Create a base report at the beginning of each month.
      // Check if there is a base report for this month or not.
      if (clusterMapConfig.clustermapEnableAggregatedMonthlyAccountReport
          && statsReportType == StatsReportType.ACCOUNT_REPORT) {
        // Get the month, if not the same month, then copy the aggregated stats and update the month
        String currentMonthValue =
            LocalDateTime.ofEpochSecond(time.seconds(), 0, zoneOffset).format(TIMESTAMP_FORMATTER);
        String recordedMonthValue = accountStatsStore.queryRecordedMonth(clusterMapConfig.clusterMapClusterName);
        if (recordedMonthValue == null || recordedMonthValue.isEmpty() || !currentMonthValue.equals(
            recordedMonthValue)) {
          accountStatsStore.takeSnapshotOfAggregatedStatsAndUpdateMonth(clusterMapConfig.clusterMapClusterName,
              currentMonthValue);
        }
      }
      return new TaskResult(TaskResult.Status.COMPLETED, "Aggregation success");
    } catch (Exception e) {
      logger.error("Exception thrown while aggregating stats from container stats reports across all nodes ", e);
      exception = e;
      return new TaskResult(TaskResult.Status.FAILED, "Exception thrown");
    } finally {
      if (clusterMapConfig.clustermapEnableContainerDeletionAggregation && callback != null && results != null
          && statsReportType.equals(StatsReportType.ACCOUNT_REPORT)) {
        callback.onCompletion(results.getFirst(), exception);
      }
    }
  }

  private String stripPortNumber(String instanceName) {
    int ind = instanceName.lastIndexOf("_");
    if (ind == -1) {
      return instanceName;
    } else {
      try {
        Short.valueOf(instanceName.substring(ind + 1));
      } catch (NumberFormatException e) {
        // string after "_" is not a port number, then return instance name.
        return instanceName;
      }
      return instanceName.substring(0, ind);
    }
  }

  @Override
  public void cancel() {

  }
}
