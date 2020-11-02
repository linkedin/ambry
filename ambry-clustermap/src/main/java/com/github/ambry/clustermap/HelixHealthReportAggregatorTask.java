/**
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.server.StatsReportType;
import com.github.ambry.server.StatsSnapshot;
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
import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskResult;
import org.apache.helix.task.UserContentStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.zookeeper.data.Stat;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helix task to aggregate health reports across all storage nodes and update the helix property store with the result
 */
class HelixHealthReportAggregatorTask extends UserContentStore implements Task {
  static final String AGGREGATED_REPORT_PREFIX = "Aggregated_";
  static final String AGGREGATED_MONTHLY_REPORT_SUFFIX = "_Month_Base";
  static final String RAW_VALID_SIZE_FIELD_NAME = "raw_valid_data_size";
  static final String VALID_SIZE_FIELD_NAME = "valid_data_size";
  static final String MONTH_NAME = "month";
  public static final String TASK_COMMAND_PREFIX = "aggregate";
  private static final String TIMESTAMP_FIELD_NAME = "timestamp";
  private static final String ERROR_OCCURRED_INSTANCES_FIELD_NAME = "error_occurred_instances";
  private static final ZoneOffset zoneOffset = ZoneId.systemDefault().getRules().getOffset(LocalDateTime.now());
  static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM");
  private final HelixManager manager;
  private final HelixClusterAggregator clusterAggregator;
  private final String healthReportName;
  private final String statsFieldName;
  private final StatsReportType statsReportType;
  private final ClusterMapConfig clusterMapConfig;
  private final Callback<StatsSnapshot> callback;
  private final Time time;
  private static final Logger logger = LoggerFactory.getLogger(HelixHealthReportAggregatorTask.class);

  /**
   * Instantiates {@link HelixHealthReportAggregatorTask}
   * @param context the {@link TaskCallbackContext} associated with the task
   * @param relevantTimePeriodInMs relevant time period in ms within which values are considered to be valid. Values
   *                               outside of this period will be ignored.
   * @param healthReportName Name of the health report
   * @param statsFieldName Stats field name
   * @param statsReportType the type of stats report
   * @param callback a callback which will be invoked when the aggregation report has been generated successfully.
   * @param clusterMapConfig the {@link ClusterMapConfig} associated with helix participant.
   */
  HelixHealthReportAggregatorTask(TaskCallbackContext context, long relevantTimePeriodInMs, String healthReportName,
      String statsFieldName, StatsReportType statsReportType, Callback<StatsSnapshot> callback,
      ClusterMapConfig clusterMapConfig) {
    this(context.getManager(), relevantTimePeriodInMs, healthReportName, statsFieldName, statsReportType, callback,
        clusterMapConfig, SystemTime.getInstance());
  }

  HelixHealthReportAggregatorTask(HelixManager manager, long relevantTimePeriodInMs, String healthReportName,
      String statsFieldName, StatsReportType statsReportType, Callback<StatsSnapshot> callback,
      ClusterMapConfig clusterMapConfig, Time time) {
    this.manager = manager;
    clusterAggregator = new HelixClusterAggregator(relevantTimePeriodInMs);
    this.healthReportName = healthReportName;
    this.statsFieldName = statsFieldName;
    this.statsReportType = statsReportType;
    this.callback = callback;
    this.clusterMapConfig = clusterMapConfig;
    this.time = time;
  }

  @Override
  public TaskResult run() {
    Pair<StatsSnapshot, StatsSnapshot> results = null;
    Exception exception = null;
    try {
      HelixDataAccessor helixDataAccessor = manager.getHelixDataAccessor();
      List<String> instanceNames = manager.getClusterManagmentTool().getInstancesInCluster(manager.getClusterName());
      Map<String, String> statsWrappersJSON = new HashMap<>();
      for (String instanceName : instanceNames) {
        PropertyKey.Builder keyBuilder = helixDataAccessor.keyBuilder();
        HelixProperty record = helixDataAccessor.getProperty(keyBuilder.healthReport(instanceName, healthReportName));
        if (record != null && record.getRecord() != null) {
          statsWrappersJSON.put(instanceName, record.getRecord().getSimpleField(statsFieldName));
        }
      }
      ObjectMapper mapper = new ObjectMapper();
      results = clusterAggregator.doWork(statsWrappersJSON, statsReportType);
      String resultId = String.format("%s%s", AGGREGATED_REPORT_PREFIX, healthReportName);
      ZNRecord znRecord = new ZNRecord(resultId);
      znRecord.setSimpleField(RAW_VALID_SIZE_FIELD_NAME, mapper.writeValueAsString(results.getFirst()));
      znRecord.setSimpleField(VALID_SIZE_FIELD_NAME, mapper.writeValueAsString(results.getSecond()));
      znRecord.setSimpleField(TIMESTAMP_FIELD_NAME, String.valueOf(time.milliseconds()));
      znRecord.setListField(ERROR_OCCURRED_INSTANCES_FIELD_NAME,
          clusterAggregator.getExceptionOccurredInstances(statsReportType));
      String path = String.format("/%s", resultId);
      manager.getHelixPropertyStore().set(path, znRecord, AccessOption.PERSISTENT);

      // Create a base report at the beginning of each month.
      // Check if there is a base report for this month or not.
      if (clusterMapConfig.clustermapEnableAggregatedMonthlyAccountReport
          && statsReportType == StatsReportType.ACCOUNT_REPORT) {
        resultId =
            String.format("%s%s%s", AGGREGATED_REPORT_PREFIX, healthReportName, AGGREGATED_MONTHLY_REPORT_SUFFIX);
        path = String.format("/%s", resultId);
        Stat stat = new Stat();
        ZNRecord monthlyReportZNRecord = manager.getHelixPropertyStore().get(path, stat, AccessOption.PERSISTENT);
        String currentMonthValue =
            LocalDateTime.ofEpochSecond(time.seconds(), 0, zoneOffset).format(TIMESTAMP_FORMATTER);
        if (monthlyReportZNRecord == null || !currentMonthValue.equals(
            monthlyReportZNRecord.getSimpleField(MONTH_NAME))) {
          monthlyReportZNRecord = new ZNRecord(resultId);
          monthlyReportZNRecord.setSimpleField(MONTH_NAME, currentMonthValue);
          monthlyReportZNRecord.setSimpleField(RAW_VALID_SIZE_FIELD_NAME,
              znRecord.getSimpleField(RAW_VALID_SIZE_FIELD_NAME));
          monthlyReportZNRecord.setSimpleField(VALID_SIZE_FIELD_NAME, znRecord.getSimpleField(VALID_SIZE_FIELD_NAME));
          monthlyReportZNRecord.setSimpleField(TIMESTAMP_FIELD_NAME, String.valueOf(time.milliseconds()));
          monthlyReportZNRecord.setListField(ERROR_OCCURRED_INSTANCES_FIELD_NAME,
              clusterAggregator.getExceptionOccurredInstances(statsReportType));
          manager.getHelixPropertyStore().set(path, monthlyReportZNRecord, AccessOption.PERSISTENT);
        }
      }
      return new TaskResult(TaskResult.Status.COMPLETED, "Aggregation success");
    } catch (Exception e) {
      logger.error("Exception thrown while aggregating stats from health reports across all nodes ", e);
      exception = e;
      return new TaskResult(TaskResult.Status.FAILED, "Exception thrown");
    } finally {
      if (clusterMapConfig.clustermapEnableContainerDeletionAggregation && callback != null && results != null
          && statsReportType.equals(StatsReportType.ACCOUNT_REPORT)) {
        callback.onCompletion(results.getFirst(), exception);
      }
    }
  }

  @Override
  public void cancel() {

  }
}
