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

import com.github.ambry.utils.Pair;
import java.util.ArrayList;
import java.util.List;
import org.apache.helix.HelixDataAccessor;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixProperty;
import org.apache.helix.PropertyKey;
import org.apache.helix.ZNRecord;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskResult;
import org.apache.helix.task.UserContentStore;


class HelixAggregateTask extends UserContentStore implements Task {
  public static final String TASK_COMMAND_PREFIX = "aggregate";
  private static final String RAW_VALID_SIZE_FIELD_NAME = "raw_valid_data_size";
  private static final String VALID_SIZE_FIELD_NAME = "valid_data_size";
  private final HelixManager manager;
  private final HelixClusterAggregator clusterAggregator;
  private final String healthReportId;
  private final String fieldName;

  HelixAggregateTask(TaskCallbackContext context, HelixClusterAggregator aggregator, String healthReportId, String fieldName) {
    manager = context.getManager();
    clusterAggregator = aggregator;
    this.healthReportId = healthReportId;
    this.fieldName = fieldName;
  }

  @Override
  public TaskResult run() {
    try {
      HelixDataAccessor helixDataAccessor = manager.getHelixDataAccessor();
      List<String> instanceNames = manager.getClusterManagmentTool().getInstancesInCluster(manager.getClusterName());
      List<String> statsWrappersJSON = new ArrayList<>();
      for (String instanceName : instanceNames) {
        PropertyKey.Builder keyBuilder = helixDataAccessor.keyBuilder();
        HelixProperty record = helixDataAccessor.getProperty(keyBuilder.healthReport(instanceName, healthReportId));
        if (record != null && record.getRecord() != null) {
          statsWrappersJSON.add(record.getRecord().getSimpleField(fieldName));
        }
      }
      Pair<String, String> results = clusterAggregator.doWork(statsWrappersJSON);
      String resultId = String.format("Aggregated_%s", healthReportId);
      ZNRecord znRecord = new ZNRecord(resultId);
      znRecord.setSimpleField(RAW_VALID_SIZE_FIELD_NAME, results.getFirst());
      znRecord.setSimpleField(VALID_SIZE_FIELD_NAME, results.getSecond());
      String path = String.format("/%s", resultId);
      manager.getHelixPropertyStore().set(path, znRecord, 1);
      return new TaskResult(TaskResult.Status.COMPLETED, "Aggregation success");
    } catch (Exception e) {
      return new TaskResult(TaskResult.Status.FAILED, "Exception thrown");
    }
  }

  @Override
  public void cancel() {

  }
}
