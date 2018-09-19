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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  private final Object helixAdministrationLock = new Object();
  private HelixManager manager;
  private String instanceName;
  private HelixAdmin helixAdmin;

  /**
   * Instantiate a HelixParticipant.
   * @param clusterMapConfig the {@link ClusterMapConfig} associated with this participant.
   * @param helixFactory the {@link HelixFactory} to use to get the {@link HelixManager}.
   * @throws IOException if there is an error in parsing the JSON serialized ZK connect string config.
   */
  HelixParticipant(ClusterMapConfig clusterMapConfig, HelixFactory helixFactory) throws IOException {
    clusterName = clusterMapConfig.clusterMapClusterName;
    instanceName =
        ClusterMapUtils.getInstanceName(clusterMapConfig.clusterMapHostName, clusterMapConfig.clusterMapPort);
    this.helixFactory = helixFactory;
    if (clusterName.isEmpty()) {
      throw new IllegalStateException("Clustername is empty in clusterMapConfig");
    }
    try {
      zkConnectStr = ClusterMapUtils.parseDcJsonAndPopulateDcInfo(clusterMapConfig.clusterMapDcsZkConnectStrings)
          .get(clusterMapConfig.clusterMapDatacenterName)
          .getZkConnectStr();
      // HelixAdmin is initialized in constructor allowing caller to do any administrative operations in Helix
      // before participating.
      helixAdmin = helixFactory.getHelixAdmin(zkConnectStr);
    } catch (JSONException e) {
      throw new IOException("Received JSON exception while parsing ZKInfo json string", e);
    }
    manager = helixFactory.getZKHelixManager(clusterName, instanceName, InstanceType.PARTICIPANT, zkConnectStr);
  }

  /**
   * Initiate the participation by registering via the {@link HelixManager} as a participant to the associated
   * Helix cluster.
   * @param ambryHealthReports {@link List} of {@link AmbryHealthReport} to be registered to the participant.
   * @throws IOException if there is an error connecting to the Helix cluster.
   */
  @Override
  public void participate(List<AmbryHealthReport> ambryHealthReports) throws IOException {
    logger.info("Initiating the participation");
    StateMachineEngine stateMachineEngine = manager.getStateMachineEngine();
    stateMachineEngine.registerStateModelFactory(LeaderStandbySMD.name, new AmbryStateModelFactory());
    registerHealthReportTasks(stateMachineEngine, ambryHealthReports);
    try {
      synchronized (helixAdministrationLock) {
        // close the temporary helixAdmin used in the process of starting StorageManager
        // this is to ensure there is only one valid helixAdmin
        helixAdmin.close();
        // register server as a participant
        manager.connect();
        // reassign the helixAdmin from ZKHelixManager, which is the actual helixAdmin after participation
        helixAdmin = manager.getClusterManagmentTool();
      }
    } catch (Exception e) {
      throw new IOException("Exception while connecting to the Helix manager", e);
    }
    for (AmbryHealthReport ambryHealthReport : ambryHealthReports) {
      manager.getHealthReportCollector().addHealthReportProvider((HealthReportProvider) ambryHealthReport);
    }
  }

  @Override
  public boolean setReplicaSealedState(ReplicaId replicaId, boolean isSealed) {
    if (!(replicaId instanceof AmbryReplica)) {
      throw new IllegalArgumentException(
          "HelixParticipant only works with the AmbryReplica implementation of ReplicaId");
    }
    synchronized (helixAdministrationLock) {
      List<String> sealedReplicas = new ArrayList<>(getSealedReplicas());
      String partitionId = replicaId.getPartitionId().toPathString();
      boolean success = true;
      if (!isSealed && sealedReplicas.contains(partitionId)) {
        logger.trace("Removing the partition {} from sealedReplicas list", partitionId);
        sealedReplicas.remove(partitionId);
        success = setSealedReplicas(sealedReplicas);
      } else if (isSealed && !sealedReplicas.contains(partitionId)) {
        logger.trace("Adding the partition {} to sealedReplicas list", partitionId);
        sealedReplicas.add(partitionId);
        success = setSealedReplicas(sealedReplicas);
      }
      logger.trace("Set sealed state of partition {} is completed", partitionId);
      return success;
    }
  }

  @Override
  public boolean setReplicaStoppedState(List<ReplicaId> replicaIds, boolean markStop) {
    Set<String> replicasToUpdate = new HashSet<>();
    for (ReplicaId replicaId : replicaIds) {
      if (!(replicaId instanceof AmbryReplica)) {
        throw new IllegalArgumentException(
            "HelixParticipant only works with the AmbryReplica implementation of ReplicaId");
      }
      replicasToUpdate.add(replicaId.getPartitionId().toPathString());
    }
    boolean setStoppedResult;
    synchronized (helixAdministrationLock) {
      logger.trace("Getting stopped replicas from instanceConfig");
      List<String> stoppedListInHelix = getStoppedReplicas();
      Set<String> stoppedSet = new HashSet<>(stoppedListInHelix);
      boolean stoppedSetUpdated =
          markStop ? stoppedSet.addAll(replicasToUpdate) : stoppedSet.removeAll(replicasToUpdate);
      if (stoppedSetUpdated) {
        logger.trace("Updating the stopped list in Helix InstanceConfig");
        setStoppedResult = setStoppedReplicas(new ArrayList<>(stoppedSet));
      } else {
        logger.trace("No replicas should be added or removed, no need to update the stopped list");
        setStoppedResult = true;
      }
    }
    return setStoppedResult;
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
   * Get the list of sealed replicas from the HelixAdmin.
   * @return list of sealed replicas from HelixAdmin.
   */
  @Override
  public List<String> getSealedReplicas() {
    InstanceConfig instanceConfig = helixAdmin.getInstanceConfig(clusterName, instanceName);
    if (instanceConfig == null) {
      throw new IllegalStateException(
          "No instance config found for cluster: \"" + clusterName + "\", instance: \"" + instanceName + "\"");
    }
    return ClusterMapUtils.getSealedReplicas(instanceConfig);
  }

  /**
   * Get the list of stopped replicas from the HelixAdmin.
   * @return list of stopped replicas from HelixAdmin
   */
  @Override
  public List<String> getStoppedReplicas() {
    InstanceConfig instanceConfig = helixAdmin.getInstanceConfig(clusterName, instanceName);
    if (instanceConfig == null) {
      throw new IllegalStateException(
          "No instance config found for cluster: \"" + clusterName + "\", instance: \"" + instanceName + "\"");
    }
    return ClusterMapUtils.getStoppedReplicas(instanceConfig);
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
   * Set the list of sealed replicas in the HelixAdmin. This method is called only after the helixAdministrationLock
   * is taken.
   * @param sealedReplicas list of sealed replicas to be set in the HelixAdmin
   * @return whether the operation succeeded or not
   */
  private boolean setSealedReplicas(List<String> sealedReplicas) {
    InstanceConfig instanceConfig = helixAdmin.getInstanceConfig(clusterName, instanceName);
    if (instanceConfig == null) {
      throw new IllegalStateException(
          "No instance config found for cluster: \"" + clusterName + "\", instance: \"" + instanceName + "\"");
    }
    logger.trace("Setting InstanceConfig with list of sealed replicas: {}", sealedReplicas.toArray());
    instanceConfig.getRecord().setListField(ClusterMapUtils.SEALED_STR, sealedReplicas);
    return helixAdmin.setInstanceConfig(clusterName, instanceName, instanceConfig);
  }

  /**
   * Set the list of stopped replicas in the HelixAdmin. This method is called only after the helixAdministrationLock
   * is taken.
   * @param stoppedReplicas list of stopped replicas to be set in the HelixAdmin
   * @return whether the operation succeeded or not
   */
  boolean setStoppedReplicas(List<String> stoppedReplicas) {
    InstanceConfig instanceConfig = helixAdmin.getInstanceConfig(clusterName, instanceName);
    if (instanceConfig == null) {
      throw new IllegalStateException(
          "No instance config found for cluster: \"" + clusterName + "\", instance: \"" + instanceName + "\"");
    }
    logger.trace("Setting InstanceConfig with list of stopped replicas: {}", stoppedReplicas.toArray());
    instanceConfig.getRecord().setListField(ClusterMapUtils.STOPPED_REPLICAS_STR, stoppedReplicas);
    return helixAdmin.setInstanceConfig(clusterName, instanceName, instanceConfig);
  }
}
