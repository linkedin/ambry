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

import com.github.ambry.utils.Utils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.ScheduleConfig;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.Workflow;


/**
 * This tool triggers a one-time cluster wide stats aggregation as a generic job (executed by a random live instance).
 */
public class HelixClusterWideAggregationTool {
  private static int SESSION_TIMEOUT = (int) TimeUnit.SECONDS.toMillis(30);
  private static int CONNECTION_TIMEOUT = (int) TimeUnit.SECONDS.toMillis(120);
  private static String REPORT_NAME = "QuotaReport";
  private static String TASK_ID = "aggregate_task";
  private static String ONE_TIME_JOB_ID = "one_time_aggregate_job";
  private static String RECURRENT_JOB_ID = "recurrent_aggregate_job";

  /**
   * @param args takes in three mandatory arguments: the ZK layout, the cluster name, the workflow name. Optional
   *             argument to create the workflow as a recurrent workflow and specifies the recurrent time interval.
   *             The ZK layout has to be of the following form:
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
   * @throws Exception
   */
  public static void main(String args[]) throws Exception {
    OptionParser parser = new OptionParser();

    ArgumentAcceptingOptionSpec<String> zkLayoutPathOpt = parser.accepts("zkLayoutPath",
        "The path to the json file containing zookeeper connect info. This should be of the following form: \n{\n"
            + "  \"zkInfo\" : [\n" + "     {\n" + "       \"datacenter\":\"dc1\",\n" + "       \"id\":\"1\",\n"
            + "       \"zkConnectStr\":\"abc.example.com:2199\",\n" + "     },\n" + "     {\n"
            + "       \"datacenter\":\"dc2\",\n" + "       \"id\":\"2\",\n"
            + "       \"zkConnectStr\":\"def.example.com:2300\",\n" + "     },\n" + "     {\n"
            + "       \"datacenter\":\"dc3\",\n" + "       \"id\":\"3\",\n"
            + "       \"zkConnectStr\":\"ghi.example.com:2400\",\n" + "     }\n" + "  ]\n" + "}").
        withRequiredArg().
        describedAs("zk_connect_info_path").
        ofType(String.class);

    ArgumentAcceptingOptionSpec<String> clusterNameOpt = parser.accepts("clusterName", "The cluster name in helix")
        .withRequiredArg()
        .describedAs("cluster_name")
        .ofType(String.class);

    ArgumentAcceptingOptionSpec<String> workflowNameOpt =
        parser.accepts("workflowName", "The name of the one-time workflow")
            .withRequiredArg()
            .describedAs("workflow_name")
            .ofType(String.class);

    ArgumentAcceptingOptionSpec<Long> recurrentIntervalInMinutesOpt =
        parser.accepts("recurrentIntervalInMinutes", "The frequency for the recurrent workflow")
            .withOptionalArg()
            .describedAs("recurrent_interval_in_minutes")
            .ofType(Long.class)
            .defaultsTo(Utils.Infinite_Time);

    parser.accepts("delete", "Flag to remove the given workflow from the cluster(s) instead of creating one");

    OptionSet options = parser.parse(args);
    Boolean isDelete = options.has("delete");
    String zkLayoutPath = options.valueOf(zkLayoutPathOpt);
    String clusterName = options.valueOf(clusterNameOpt);
    String workflowName = options.valueOf(workflowNameOpt);
    Long recurrentIntervalInMinutes = options.valueOf(recurrentIntervalInMinutesOpt);
    Map<String, ClusterMapUtils.DcZkInfo> dataCenterToZKAddress =
        ClusterMapUtils.parseDcJsonAndPopulateDcInfo(Utils.readStringFromFile(zkLayoutPath));
    for (ClusterMapUtils.DcZkInfo zkInfo : dataCenterToZKAddress.values()) {
      String zkAddress = zkInfo.getZkConnectStr();
      ZkClient zkClient = new ZkClient(zkAddress, SESSION_TIMEOUT, CONNECTION_TIMEOUT, new ZNRecordSerializer());
      TaskDriver taskDriver = new TaskDriver(zkClient, clusterName);
      if (isDelete) {
        try {
          taskDriver.stop(workflowName);
          taskDriver.delete(workflowName);
        } catch (Exception | Error e) {
          System.out.println(
              String.format("Failed to delete %s. Workflow not found in cluster %s at %s", workflowName, clusterName,
                  zkAddress));
        }
      } else {
        try {
          Workflow.Builder workflowBuilder = new Workflow.Builder(workflowName);
          String jobId = ONE_TIME_JOB_ID;
          if (recurrentIntervalInMinutes != Utils.Infinite_Time) {
            jobId = RECURRENT_JOB_ID;
            workflowBuilder.setScheduleConfig(
                ScheduleConfig.recurringFromNow(TimeUnit.MINUTES, recurrentIntervalInMinutes));
            workflowBuilder.setExpiry(TimeUnit.MINUTES.toMillis(recurrentIntervalInMinutes));
          }
          JobConfig.Builder jobConfigBuilder = new JobConfig.Builder();
          List<TaskConfig> taskConfigs = new ArrayList<>();
          taskConfigs.add(new TaskConfig.Builder().setTaskId(TASK_ID)
              .setCommand(String.format("%s_%s", HelixHealthReportAggregatorTask.TASK_COMMAND_PREFIX, REPORT_NAME))
              .build());
          jobConfigBuilder.addTaskConfigs(taskConfigs);
          workflowBuilder.addJob(jobId, jobConfigBuilder);
          Workflow workflow = workflowBuilder.build();
          taskDriver.start(workflow);
          System.out.println(String.format("%s_%s started successfully", workflowName, jobId));
        } catch (Exception | Error e) {
          System.out.println(
              String.format("Failed to start %s in cluster %s at %s", workflowName, clusterName, zkAddress));
        }
      }
    }
  }
}
