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
  private final String quotaStatsFieldName;
  private static final Logger logger = LoggerFactory.getLogger(HelixHealthReportAggregatorTask.class);

  /**
   * Instantiates {@link HelixHealthReportAggregatorTask}
   * @param context the {@link TaskCallbackContext} associated with the task
   * @param relevantTimePeriodInMs relevant time period in ms within which values are considered to be valid. Values
   *                               outside of this period will be ignored.
   * @param healthReportName Name of the health report
   * @param quotaStatsFieldName Quota stats field name
   */
  HelixHealthReportAggregatorTask(TaskCallbackContext context, long relevantTimePeriodInMs, String healthReportName,
      String quotaStatsFieldName) {
    manager = context.getManager();
    clusterAggregator = new HelixClusterAggregator(relevantTimePeriodInMs);
    this.healthReportName = healthReportName;
    this.quotaStatsFieldName = quotaStatsFieldName;
  }

  @Override
  public TaskResult run() {
    try {
      HelixDataAccessor helixDataAccessor = manager.getHelixDataAccessor();
      List<String> instanceNames = manager.getClusterManagmentTool().getInstancesInCluster(manager.getClusterName());
      Map<String, String> partitionClassStatsWrappers = new HashMap<>();
      Map<String, String> accountStatsWrappers = new HashMap<>();
      for (String instanceName : instanceNames) {
        PropertyKey.Builder keyBuilder = helixDataAccessor.keyBuilder();
        HelixProperty record = helixDataAccessor.getProperty(keyBuilder.healthReport(instanceName, healthReportName));
        if (record != null && record.getRecord() != null) {
          partitionClassStatsWrappers.put(instanceName, record.getRecord().getSimpleField("PartitionClassStats"));
          accountStatsWrappers.put(instanceName, record.getRecord().getSimpleField("AccountStats"));
        }
      }
      Pair<String, String> partitionClassResults =
          clusterAggregator.doWork(partitionClassStatsWrappers, StatsReportType.PARTITION_CLASS_REPORT);
      Pair<String, String> accountResults =
          clusterAggregator.doWork(accountStatsWrappers, StatsReportType.ACCOUNT_REPORT);
      String partitionClassResultId = String.format("PartitionClass_Aggregated_%s", healthReportName);
      String accountResultId = String.format("Account_Aggregated_%s", healthReportName);
      ZNRecord partitionClassZNRecord = new ZNRecord(partitionClassResultId);
      ZNRecord accountZNRecord = new ZNRecord(accountResultId);

      partitionClassZNRecord.setVersion(0);
      partitionClassZNRecord.setSimpleField(RAW_VALID_SIZE_FIELD_NAME, partitionClassResults.getFirst());
      partitionClassZNRecord.setSimpleField(VALID_SIZE_FIELD_NAME, partitionClassResults.getSecond());
      partitionClassZNRecord.setSimpleField(TIMESTAMP_FIELD_NAME,
          String.valueOf(SystemTime.getInstance().milliseconds()));
      partitionClassZNRecord.setListField(ERROR_OCCURRED_INSTANCES_FIELD_NAME,
          clusterAggregator.getExceptionOccurredInstances(StatsReportType.PARTITION_CLASS_REPORT));

      accountZNRecord.setVersion(0);
      accountZNRecord.setSimpleField(RAW_VALID_SIZE_FIELD_NAME, accountResults.getFirst());
      accountZNRecord.setSimpleField(VALID_SIZE_FIELD_NAME, accountResults.getSecond());
      accountZNRecord.setSimpleField(TIMESTAMP_FIELD_NAME, String.valueOf(SystemTime.getInstance().milliseconds()));
      accountZNRecord.setListField(ERROR_OCCURRED_INSTANCES_FIELD_NAME,
          clusterAggregator.getExceptionOccurredInstances(StatsReportType.ACCOUNT_REPORT));

      String partitionClassPath = String.format("/%s", partitionClassResultId);
      String accountPath = String.format("/%s", accountResultId);

      manager.getHelixPropertyStore().set(partitionClassPath, partitionClassZNRecord, AccessOption.PERSISTENT);
      manager.getHelixPropertyStore().set(accountPath, accountZNRecord, AccessOption.PERSISTENT);

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
