/*
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

import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.server.AmbryHealthReport;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.healthcheck.HealthReportProvider;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LeaderStandbySMD;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskStateModelFactory;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An implementation of {@link ClusterParticipant} that registers as a participant to a Helix cluster.
 */
class HelixParticipant implements ClusterParticipant {
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final String clusterName;
  private final String zkConnectStr;
  private final HelixFactory helixFactory;
  private HelixManager manager;
  private String instanceName;

  /**
   * Instantiate a HelixParticipant.
   * @param clusterMapConfig the {@link ClusterMapConfig} associated with this participant.
   * @param helixFactory the {@link HelixFactory} to use to get the {@link HelixManager}.
   * @throws JSONException if there is an error in parsing the JSON serialized ZK connect string config.
   */
  HelixParticipant(ClusterMapConfig clusterMapConfig, HelixFactory helixFactory) throws IOException {
    clusterName = clusterMapConfig.clusterMapClusterName;
    this.helixFactory = helixFactory;
    if (clusterName.isEmpty()) {
      throw new IllegalStateException("Clustername is empty in clusterMapConfig");
    }
    try {
      zkConnectStr = ClusterMapUtils.parseDcJsonAndPopulateDcInfo(clusterMapConfig.clusterMapDcsZkConnectStrings)
          .get(clusterMapConfig.clusterMapDatacenterName)
          .getZkConnectStr();
    } catch (JSONException e) {
      throw new IOException("Received JSON exception while parsing ZKInfo json string", e);
    }
  }

  /**
   * Initialize the participant by registering via the {@link HelixManager} as a participant to the associated Helix
   * cluster.
   * @param hostName the hostname to use when registering as a participant.
   * @param port the port to use when registering as a participant.
   * @param ambryHealthReports {@link List} of {@link AmbryHealthReport} to be registered to the participant.
   * @throws IOException if there is an error connecting to the Helix cluster.
   */
  @Override
  public void initialize(String hostName, int port, List<AmbryHealthReport> ambryHealthReports) throws IOException {
    logger.info("Initializing participant");
    instanceName = ClusterMapUtils.getInstanceName(hostName, port);
    manager = helixFactory.getZKHelixManager(clusterName, instanceName, InstanceType.PARTICIPANT, zkConnectStr);
    StateMachineEngine stateMachineEngine = manager.getStateMachineEngine();
    stateMachineEngine.registerStateModelFactory(LeaderStandbySMD.name, new AmbryStateModelFactory());
    registerHealthReportTasks(stateMachineEngine, ambryHealthReports);
    try {
      manager.connect();
    } catch (Exception e) {
      throw new IOException("Exception while connecting to the Helix manager", e);
    }
    for (AmbryHealthReport ambryHealthReport : ambryHealthReports) {
      manager.getHealthReportCollector().addHealthReportProvider((HealthReportProvider) ambryHealthReport);
    }
  }

  @Override
  public synchronized boolean setReplicaSealedState(ReplicaId replicaId, boolean isSealed) {
    if (!(replicaId instanceof AmbryReplica)) {
      throw new IllegalArgumentException(
          "HelixParticipant only works with the AmbryReplica implementation of ReplicaId");
    }
    List<String> sealedReplicas = getSealedReplicas();
    if (sealedReplicas == null) {
      sealedReplicas = new ArrayList<>();
    }
    String partitionId = replicaId.getPartitionId().toPathString();
    boolean success = true;
    if (!isSealed && sealedReplicas.contains(partitionId)) {
      sealedReplicas.remove(partitionId);
      success = setSealedReplicas(sealedReplicas);
    } else if (isSealed && !sealedReplicas.contains(partitionId)) {
      sealedReplicas.add(partitionId);
      success = setSealedReplicas(sealedReplicas);
    }
    return success;
  }

  /**
   * Disconnect from the {@link HelixManager}.
   */
  @Override
  public void close() {
    if (manager != null) {
      manager.disconnect();
      manager = null;
    }
  }

  /**
   * Register {@link HelixHealthReportAggregatorTask}s for appropriate {@link AmbryHealthReport}s.
   * @param engine the {@link StateMachineEngine} to register the task state model.
   * @param healthReports the {@link List} of {@link AmbryHealthReport}s that may require the registration of
   * corresponding {@link HelixHealthReportAggregatorTask}s.
   */
  private void registerHealthReportTasks(StateMachineEngine engine, List<AmbryHealthReport> healthReports) {
    Map<String, TaskFactory> taskFactoryMap = new HashMap<>();
    for (final AmbryHealthReport healthReport : healthReports) {
      if (healthReport.getAggregateIntervalInMinutes() != Utils.Infinite_Time) {
        // register cluster wide aggregation task for the health report
        taskFactoryMap.put(
            String.format("%s_%s", HelixHealthReportAggregatorTask.TASK_COMMAND_PREFIX, healthReport.getReportName()),
            new TaskFactory() {
              @Override
              public Task createNewTask(TaskCallbackContext context) {
                return new HelixHealthReportAggregatorTask(context, healthReport.getAggregateIntervalInMinutes(),
                    healthReport.getReportName(), healthReport.getQuotaStatsFieldName());
              }
            });
      }
    }
    if (!taskFactoryMap.isEmpty()) {
      engine.registerStateModelFactory(TaskConstants.STATE_MODEL_NAME,
          new TaskStateModelFactory(manager, taskFactoryMap));
    }
  }

  /**
   * Get the list of sealed replicas from the HelixAdmin
   * @return list of sealed replicas from HelixAdmin
   */
  private List<String> getSealedReplicas() {
    HelixAdmin helixAdmin = manager.getClusterManagmentTool();
    InstanceConfig instanceConfig = helixAdmin.getInstanceConfig(clusterName, instanceName);
    if (instanceConfig == null) {
      throw new IllegalStateException(
          "No instance config found for cluster: \"" + clusterName + "\", instance: \"" + instanceName + "\"");
    }
    return ClusterMapUtils.getSealedReplicas(instanceConfig);
  }

  /**
   * Set the list of sealed replicas in the HelixAdmin
   * @param sealedReplicas list of sealed replicas to be set in the HelixAdmin
   * @return whether the operation succeeded or not
   */
  private boolean setSealedReplicas(List<String> sealedReplicas) {
    HelixAdmin helixAdmin = manager.getClusterManagmentTool();
    InstanceConfig instanceConfig = helixAdmin.getInstanceConfig(clusterName, instanceName);
    if (instanceConfig == null) {
      throw new IllegalStateException(
          "No instance config found for cluster: \"" + clusterName + "\", instance: \"" + instanceName + "\"");
    }
    instanceConfig.getRecord().setListField(ClusterMapUtils.SEALED_STR, sealedReplicas);
    return helixAdmin.setInstanceConfig(clusterName, instanceName, instanceConfig);
  }
}
