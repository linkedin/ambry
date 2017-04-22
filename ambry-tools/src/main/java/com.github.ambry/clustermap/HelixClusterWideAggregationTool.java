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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.helix.manager.zk.ZNRecordSerializer;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.task.JobConfig;
import org.apache.helix.task.TaskConfig;
import org.apache.helix.task.TaskDriver;
import org.apache.helix.task.TaskState;
import org.apache.helix.task.Workflow;


/**
 * This tool triggers a one-time cluster wide stats aggregation as a generic job (executed by a random live instance).
 */
public class HelixClusterWideAggregationTool {
  private static long WORKFLOW_EXPIRY = TimeUnit.SECONDS.toMillis(30);
  private static int SESSION_TIMEOUT = (int) TimeUnit.SECONDS.toMillis(30);
  private static int CONNECTION_TIMEOUT = (int) TimeUnit.SECONDS.toMillis(120);
  private static String REPORT_NAME = "QuotaReport";
  private static String TASK_ID = "aggregate_task";
  private static String JOB_ID = "one_time_aggregate_job";

  /**
   * @param args takes in three mandatory arguments: the zookeeper address, the cluster name and the workflow name.
   * @throws Exception
   */
  public static void main(String args[]) throws Exception {
    OptionParser parser = new OptionParser();

    ArgumentAcceptingOptionSpec<String> zkAddressOpt =
        parser.accepts("zkAddress", "The zookeeper address to connect to")
            .withRequiredArg()
            .describedAs("zk_address")
            .ofType(String.class);

    ArgumentAcceptingOptionSpec<String> clusterNameOpt = parser.accepts("clusterName", "The cluster name in helix")
        .withRequiredArg()
        .describedAs("cluster_name")
        .ofType(String.class);

    ArgumentAcceptingOptionSpec<String> workflowNameOpt =
        parser.accepts("workflowName", "The name of the one-time workflow")
            .withRequiredArg()
            .describedAs("workflow_name")
            .ofType(String.class);

    ArgumentAcceptingOptionSpec<Long> taskTimeoutInSecsOpt =
        parser.accepts("taskTimeoutInSecs", "Timeout for the task to complete")
            .withRequiredArg()
            .describedAs("task_timeout_in_secs")
            .ofType(Long.class);

    OptionSet options = parser.parse(args);
    String zkAddress = options.valueOf(zkAddressOpt);
    String clusterName = options.valueOf(clusterNameOpt);
    String workflowName = options.valueOf(workflowNameOpt);
    Long taskTimeoutInSecs = options.valueOf(taskTimeoutInSecsOpt);
    ZkClient zkClient = new ZkClient(zkAddress, SESSION_TIMEOUT, CONNECTION_TIMEOUT, new ZNRecordSerializer());
    TaskDriver taskDriver = new TaskDriver(zkClient, clusterName);
    Workflow.Builder workflowBuilder = new Workflow.Builder(workflowName);
    workflowBuilder.setExpiry(WORKFLOW_EXPIRY);
    JobConfig.Builder jobConfigBuilder = new JobConfig.Builder();
    List<TaskConfig> taskConfigs = new ArrayList<>();
    taskConfigs.add(new TaskConfig.Builder().setTaskId(TASK_ID)
        .setCommand(String.format("%s_%s", HelixAggregateTask.TASK_COMMAND_PREFIX, REPORT_NAME))
        .build());
    jobConfigBuilder.addTaskConfigs(taskConfigs);
    workflowBuilder.addJob(JOB_ID, jobConfigBuilder);
    Workflow workflow = workflowBuilder.build();
    taskDriver.start(workflow);
    String jobName = String.format("%s_%s", workflowName, JOB_ID);
    taskDriver.pollForJobState(workflowName, jobName, taskTimeoutInSecs, TaskState.COMPLETED);
  }
}
