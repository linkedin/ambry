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

import com.github.ambry.server.StatsReportType;
import com.github.ambry.utils.Pair;
import com.github.ambry.utils.SystemTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskResult;
import org.apache.helix.task.UserContentStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helix task to aggregate health reports across all storage nodes and update the helix property store with the result
 */
class HelixHealthReportAggregatorTask extends UserContentStore implements Task {
  public static final String TASK_COMMAND_PREFIX = "aggregate";
  private static final String RAW_VALID_SIZE_FIELD_NAME = "raw_valid_data_size";
  private static final String VALID_SIZE_FIELD_NAME = "valid_data_size";
  private static final String TIMESTAMP_FIELD_NAME = "timestamp";
  private static final String ERROR_OCCURRED_INSTANCES_FIELD_NAME = "error_occurred_instances";
  private final HelixManager manager;
  private final HelixClusterAggregator clusterAggregator;
  private final String healthReportName;
  private final String statsFieldName;
  private final StatsReportType statsReportType;
  private static final Logger logger = LoggerFactory.getLogger(HelixHealthReportAggregatorTask.class);

  /**
   * Instantiates {@link HelixHealthReportAggregatorTask}
   * @param context the {@link TaskCallbackContext} associated with the task
   * @param relevantTimePeriodInMs relevant time period in ms within which values are considered to be valid. Values
   *                               outside of this period will be ignored.
   * @param healthReportName Name of the health report
   * @param statsFieldName Stats field name
   * @param statsReportType the type of stats report
   */
  HelixHealthReportAggregatorTask(TaskCallbackContext context, long relevantTimePeriodInMs, String healthReportName,
      String statsFieldName, StatsReportType statsReportType) {
    manager = context.getManager();
    clusterAggregator = new HelixClusterAggregator(relevantTimePeriodInMs);
    this.healthReportName = healthReportName;
    this.statsFieldName = statsFieldName;
    this.statsReportType = statsReportType;
  }

  @Override
  public TaskResult run() {
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
      Pair<String, String> results = clusterAggregator.doWork(statsWrappersJSON, statsReportType);
      String resultId = String.format("Aggregated_%s", healthReportName);
      ZNRecord znRecord = new ZNRecord(resultId);
      znRecord.setSimpleField(RAW_VALID_SIZE_FIELD_NAME, results.getFirst());
      znRecord.setSimpleField(VALID_SIZE_FIELD_NAME, results.getSecond());
      znRecord.setSimpleField(TIMESTAMP_FIELD_NAME, String.valueOf(SystemTime.getInstance().milliseconds()));
      znRecord.setListField(ERROR_OCCURRED_INSTANCES_FIELD_NAME,
          clusterAggregator.getExceptionOccurredInstances(statsReportType));
      String path = String.format("/%s", resultId);
      manager.getHelixPropertyStore().set(path, znRecord, AccessOption.PERSISTENT);
      return new TaskResult(TaskResult.Status.COMPLETED, "Aggregation success");
    } catch (Exception e) {
      logger.error("Exception thrown while aggregating stats from health reports across all nodes ", e);
      return new TaskResult(TaskResult.Status.FAILED, "Exception thrown");
    }
  }

  @Override
  public void cancel() {

  }
}
