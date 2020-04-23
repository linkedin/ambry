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

import com.github.ambry.config.Config;
import com.github.ambry.config.Default;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.server.AmbryStatsReport;
import com.github.ambry.server.StatsReportType;
import com.github.ambry.tools.util.ToolUtils;
import com.github.ambry.utils.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.ScheduleConfig;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.Workflow;


/**
 * This tool triggers a one-time/recurrent cluster wide stats aggregation as a generic job (executed by a random live instance).
 */
public class HelixClusterWideAggregationTool {
  private static int SESSION_TIMEOUT = (int) TimeUnit.SECONDS.toMillis(30);
  private static int CONNECTION_TIMEOUT = (int) TimeUnit.SECONDS.toMillis(120);
  private static String TASK_SUFFIX = "_aggregate_task";
  private static String ONE_TIME_JOB_SUFFIX = "_one_time";
  private static String RECURRENT_JOB_SUFFIX = "_recurrent";
  private static long TIME_OUT_MILLI_SEC = 10000L;

  /**
   * Config for the HelixClusterWideAggregationTool.
   */
  private static class AggregationToolConfig {

    /**
     * The path to the zookeeper layout file.
     * The ZK layout has to be of the following form:
     *             {
     *               "zkInfo" : [
     *                 {
     *                   "datacenter":"dc1",
     *                    "id" : "1",
     *                   "zkConnectStr":"abc.example.com:2199",
     *                 },
     *                 {
     *                   "datacenter":"dc2",
     *                   "id" : "2",
     *                   "zkConnectStr":"def.example.com:2300",
     *                 }
     *               ]
     *             }
     */
    @Config("zk.layout.file.path")
    @Default("")
    final String zkLayoutFilePath;

    /**
     * The cluster name in helix
     */
    @Config("cluster.name")
    @Default("")
    final String clusterName;

    /**
     * The name of the one-time/recurrent workflow
     */
    @Config("workflow.name")
    @Default("")
    final String workflowName;

    /**
     * The type of stats report {@link StatsReportType} to aggregate
     */
    @Config("stats.reports.to.aggregate")
    @Default("")
    final List<String> statsReportsToAggregate;

    /**
     * The period of recurrent workflow in minutes
     */
    @Config("recurrent.interval.in.minutes")
    @Default("Long.MAX_VALUE")
    final long recurrentIntervalInMinutes;

    /**
     * Flag to remove the specified workflow from the cluster(s) instead of creating one
     */
    @Config("delete.specified.workflow")
    @Default("false")
    final boolean deleteSpecifiedWorkflow;

    /**
     * Constructs the configs associated with the tool.
     * @param verifiableProperties the props to use to load the config.
     */
    AggregationToolConfig(VerifiableProperties verifiableProperties) {
      zkLayoutFilePath = verifiableProperties.getString("zk.layout.file.path", "");
      clusterName = verifiableProperties.getString("cluster.name", "");
      workflowName = verifiableProperties.getString("workflow.name", "");
      recurrentIntervalInMinutes =
          verifiableProperties.getLongInRange("recurrent.interval.in.minutes", Long.MAX_VALUE, 1, Long.MAX_VALUE);
      deleteSpecifiedWorkflow = verifiableProperties.getBoolean("delete.specified.workflow", false);
      statsReportsToAggregate =
          Arrays.asList(verifiableProperties.getString("stats.reports.to.aggregate", "").split(","));
    }
  }

  /**
   * Runs the cluster wide aggregation tool
   * @param args arguments specifying config file. For example: --propsFile /path/AggregationToolConfig
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    VerifiableProperties verifiableProperties = ToolUtils.getVerifiableProperties(args);
    AggregationToolConfig config = new AggregationToolConfig(verifiableProperties);
    Map<String, ClusterMapUtils.DcZkInfo> dataCenterToZKAddress =
        ClusterMapUtils.parseDcJsonAndPopulateDcInfo(Utils.readStringFromFile(config.zkLayoutFilePath));
    String clusterName = config.clusterName;
    String workflowName = config.workflowName;
    long recurrentIntervalInMinutes = config.recurrentIntervalInMinutes;
    boolean isDelete = config.deleteSpecifiedWorkflow;
    boolean isRecurrentWorkflow = recurrentIntervalInMinutes != Utils.Infinite_Time;
    for (ClusterMapUtils.DcZkInfo zkInfo : dataCenterToZKAddress.values()) {
      // If there are multiple ZK endpoints in same dc, we trigger stats aggregation for each of them.
      for (String zkAddress : zkInfo.getZkConnectStrs()) {
        ZkClient zkClient = new ZkClient(zkAddress, SESSION_TIMEOUT, CONNECTION_TIMEOUT, new ZNRecordSerializer());
        TaskDriver taskDriver = new TaskDriver(zkClient, clusterName);
        if (isDelete) {
          try {
            taskDriver.waitToStop(workflowName, TIME_OUT_MILLI_SEC);
            taskDriver.delete(workflowName);
            System.out.println(
                String.format("Successfully deleted the workflow: %s in cluster %s at %s", workflowName, clusterName,
                    zkAddress));
          } catch (Exception | Error e) {
            System.out.println(
                String.format("Failed to delete %s. Workflow not found in cluster %s at %s", workflowName, clusterName,
                    zkAddress));
          }
        } else {
          Workflow.Builder workflowBuilder = new Workflow.Builder(workflowName);
          try {
            // create separate job for each type of stats report
            for (String report : config.statsReportsToAggregate) {
              StatsReportType statsType = StatsReportType.valueOf(report);
              String reportName = AmbryStatsReport.convertStatsReportTypeToProperString(statsType)
                  + AmbryStatsReport.REPORT_NAME_SUFFIX;
              String jobId = statsType.toString().toLowerCase() + (isRecurrentWorkflow ? RECURRENT_JOB_SUFFIX
                  : ONE_TIME_JOB_SUFFIX);
              String taskId = statsType.toString().toLowerCase() + TASK_SUFFIX;
              String aggregationCommand =
                  String.format("%s_%s", HelixHealthReportAggregatorTask.TASK_COMMAND_PREFIX, reportName);
              // build task
              List<TaskConfig> taskConfigs = new ArrayList<>();
              taskConfigs.add(new TaskConfig.Builder().setTaskId(taskId).setCommand(aggregationCommand).build());
              // build job
              JobConfig.Builder jobConfigBuilder = new JobConfig.Builder();
              jobConfigBuilder.addTaskConfigs(taskConfigs);
              jobConfigBuilder.setCommand(aggregationCommand);
              // add job into workflow
              workflowBuilder.addJob(jobId, jobConfigBuilder);
            }
            if (isRecurrentWorkflow) {
              workflowBuilder.setScheduleConfig(
                  ScheduleConfig.recurringFromNow(TimeUnit.MINUTES, recurrentIntervalInMinutes));
              workflowBuilder.setExpiry(TimeUnit.MINUTES.toMillis(recurrentIntervalInMinutes));
            }
            Workflow workflow = workflowBuilder.build();
            taskDriver.start(workflow);
            System.out.println(
                String.format("%s started successfully in cluster %s at %s", workflowName, clusterName, zkAddress));
          } catch (Exception | Error e) {
            System.out.println(
                String.format("Failed to start %s in cluster %s at %s", workflowName, clusterName, zkAddress));
          }
        }
      }
    }
  }
}
