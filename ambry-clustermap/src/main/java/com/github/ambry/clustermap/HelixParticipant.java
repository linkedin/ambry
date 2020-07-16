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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.commons.Callback;
import com.github.ambry.commons.CommonUtils;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.HelixPropertyStoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.server.AmbryHealthReport;
import com.github.ambry.server.StatsSnapshot;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.healthcheck.HealthReportProvider;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskStateModelFactory;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.clustermap.ClusterMapUtils.*;
import static com.github.ambry.clustermap.StateTransitionException.TransitionErrorCode.*;


/**
 * An implementation of {@link ClusterParticipant} that registers as a participant to a Helix cluster.
 */
public class HelixParticipant implements ClusterParticipant, PartitionStateChangeListener {
  protected final HelixParticipantMetrics participantMetrics;
  private final String clusterName;
  private final String zkConnectStr;
  private final Object helixAdministrationLock = new Object();
  private final ClusterMapConfig clusterMapConfig;
  private final Map<String, ReplicaState> localPartitionAndState = new ConcurrentHashMap<>();
  private HelixManager manager;
  private String instanceName;
  private HelixAdmin helixAdmin;
  private ReplicaSyncUpManager replicaSyncUpManager;
  private volatile boolean disablePartitionsComplete = false;
  final Map<StateModelListenerType, PartitionStateChangeListener> partitionStateChangeListeners;

  private static final Logger logger = LoggerFactory.getLogger(HelixParticipant.class);

  /**
   * Instantiate a HelixParticipant.
   * @param clusterMapConfig the {@link ClusterMapConfig} associated with this participant.
   * @param helixFactory the {@link HelixFactory} to use to get the {@link HelixManager}.
   * @param metricRegistry the {@link MetricRegistry} to instantiate {@link HelixParticipantMetrics}.
   * @param zkConnectStr the address identifying the zk service which this participant interacts with.
   * @param isSoleParticipant whether this is the sole participant on current node.
   */
  public HelixParticipant(ClusterMapConfig clusterMapConfig, HelixFactory helixFactory, MetricRegistry metricRegistry,
      String zkConnectStr, boolean isSoleParticipant) {
    this.clusterMapConfig = clusterMapConfig;
    this.zkConnectStr = zkConnectStr;
    participantMetrics =
        new HelixParticipantMetrics(metricRegistry, isSoleParticipant ? null : zkConnectStr, localPartitionAndState);
    clusterName = clusterMapConfig.clusterMapClusterName;
    instanceName = getInstanceName(clusterMapConfig.clusterMapHostName, clusterMapConfig.clusterMapPort);
    if (clusterName.isEmpty()) {
      throw new IllegalStateException("Cluster name is empty in clusterMapConfig");
    }
    // HelixAdmin is initialized in constructor allowing caller to do any administrative operations in Helix
    // before participating.
    helixAdmin = helixFactory.getHelixAdmin(this.zkConnectStr);
    manager = helixFactory.getZKHelixManager(clusterName, instanceName, InstanceType.PARTICIPANT, this.zkConnectStr);
    replicaSyncUpManager = new AmbryReplicaSyncUpManager(clusterMapConfig);
    partitionStateChangeListeners = new HashMap<>();
  }

  @Override
  public void setInitialLocalPartitions(Collection<String> localPartitions) {
    localPartitions.forEach(p -> localPartitionAndState.put(p, ReplicaState.OFFLINE));
  }

  /**
   * Initiate the participation by registering via the {@link HelixManager} as a participant to the associated
   * Helix cluster.
   * @param ambryHealthReports {@link List} of {@link AmbryHealthReport} to be registered to the participant.
   * @param callback a callback which will be invoked when the aggregation report has been generated successfully.
   * @throws IOException if there is an error connecting to the Helix cluster.
   */
  @Override
  public void participate(List<AmbryHealthReport> ambryHealthReports, Callback<StatsSnapshot> callback)
      throws IOException {
    logger.info("Initiating the participation. The specified state model is {}",
        clusterMapConfig.clustermapStateModelDefinition);
    StateMachineEngine stateMachineEngine = manager.getStateMachineEngine();
    stateMachineEngine.registerStateModelFactory(clusterMapConfig.clustermapStateModelDefinition,
        new AmbryStateModelFactory(clusterMapConfig, this));
    registerHealthReportTasks(stateMachineEngine, ambryHealthReports, callback);
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
    logger.info("Completed participation in cluster {} at {}", clusterName, zkConnectStr);
  }

  @Override
  public void registerPartitionStateChangeListener(StateModelListenerType listenerType,
      PartitionStateChangeListener partitionStateChangeListener) {
    partitionStateChangeListeners.put(listenerType, partitionStateChangeListener);
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
      logger.info("Getting stopped replicas from instanceConfig");
      List<String> stoppedListInHelix = getStoppedReplicas();
      Set<String> stoppedSet = new HashSet<>(stoppedListInHelix);
      boolean stoppedSetUpdated =
          markStop ? stoppedSet.addAll(replicasToUpdate) : stoppedSet.removeAll(replicasToUpdate);
      if (stoppedSetUpdated) {
        logger.info("Updating the stopped list in Helix InstanceConfig");
        setStoppedResult = setStoppedReplicas(new ArrayList<>(stoppedSet));
      } else {
        logger.info("No replicas should be added or removed, no need to update the stopped list");
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
   * @return list of sealed replicas from HelixAdmin.
   */
  @Override
  public List<String> getSealedReplicas() {
    return ClusterMapUtils.getSealedReplicas(getInstanceConfig());
  }

  /**
   * @return list of stopped replicas from HelixAdmin
   */
  @Override
  public List<String> getStoppedReplicas() {
    return ClusterMapUtils.getStoppedReplicas(getInstanceConfig());
  }

  @Override
  public List<String> getDisabledReplicas() {
    return ClusterMapUtils.getDisabledReplicas(getInstanceConfig());
  }

  @Override
  public ReplicaSyncUpManager getReplicaSyncUpManager() {
    return replicaSyncUpManager;
  }

  @Override
  public boolean updateDataNodeInfoInCluster(ReplicaId replicaId, boolean shouldExist) {
    boolean updateResult = true;
    if (clusterMapConfig.clustermapUpdateDatanodeInfo) {
      synchronized (helixAdministrationLock) {
        updateResult = shouldExist ? addNewReplicaInfo(replicaId) : removeOldReplicaInfo(replicaId);
      }
    }
    return updateResult;
  }

  /**
   * @return a snapshot of registered state change listeners.
   */
  @Override
  public Map<StateModelListenerType, PartitionStateChangeListener> getPartitionStateChangeListeners() {
    return Collections.unmodifiableMap(partitionStateChangeListeners);
  }

  @Override
  public void setReplicaDisabledState(ReplicaId replicaId, boolean disable) {
    if (!(replicaId instanceof AmbryReplica)) {
      throw new IllegalArgumentException(
          "HelixParticipant only works with the AmbryReplica implementation of ReplicaId");
    }
    synchronized (helixAdministrationLock) {
      List<String> disabledReplicas = new ArrayList<>(getDisabledReplicas());
      String partitionName = replicaId.getPartitionId().toPathString();

      // 1. invoke Helix native method to enable/disable partition on local node, this will trigger subsequent state
      //    transition on given replica. This method modifies MapFields in InstanceConfig.
      InstanceConfig instanceConfig = getInstanceConfig();
      String resourceNameForPartition = getResourceNameOfPartition(helixAdmin, clusterName, partitionName);
      logger.info("{} replica {} on current node", disable ? "Disabling" : "Enabling", partitionName);
      instanceConfig.setInstanceEnabledForPartition(resourceNameForPartition, partitionName, !disable);

      // 2. update disabled replica list in InstanceConfig. This modifies ListFields only
      if (!disable && disabledReplicas.contains(partitionName)) {
        logger.info("Removing the partition {} from disabledReplicas list", partitionName);
        disabledReplicas.remove(partitionName);
      } else if (disable && !disabledReplicas.contains(partitionName)) {
        logger.info("Adding the partition {} to disabledReplicas list", partitionName);
        disabledReplicas.add(partitionName);
      }
      logger.info("Setting InstanceConfig with list of disabled replicas: {}", disabledReplicas.toArray());
      instanceConfig.getRecord().setListField(DISABLED_REPLICAS_STR, disabledReplicas);

      // 3. set InstanceConfig in Helix to persist replica disabled state
      helixAdmin.setInstanceConfig(clusterName, instanceName, instanceConfig);
      logger.info("Disabled state of partition {} is updated", partitionName);
    }
  }

  /**
   * @return {@link HelixAdmin} that manages current data node.
   */
  public HelixAdmin getHelixAdmin() {
    return helixAdmin;
  }

  /**
   * Mark disablePartitionsComplete = true, this is exposed for testing only.
   */
  protected void markDisablePartitionComplete() {
    disablePartitionsComplete = true;
  }

  /**
   * Add new replica info into {@link InstanceConfig} of current data node.
   * @param replicaId new replica whose info should be added into {@link InstanceConfig}.
   * @return {@code true} replica info is successfully added. {@code false} otherwise.
   */
  private boolean addNewReplicaInfo(ReplicaId replicaId) {
    boolean additionResult = true;
    String partitionName = replicaId.getPartitionId().toPathString();
    String newReplicaInfo =
        String.join(REPLICAS_STR_SEPARATOR, partitionName, String.valueOf(replicaId.getCapacityInBytes()),
            replicaId.getPartitionId().getPartitionClass()) + REPLICAS_DELIM_STR;
    InstanceConfig instanceConfig = getInstanceConfig();
    Map<String, Map<String, String>> mountPathToDiskInfos = instanceConfig.getRecord().getMapFields();
    Map<String, String> diskInfo = mountPathToDiskInfos.get(replicaId.getMountPath());
    Map<String, String> diskInfoToAdd;
    boolean newReplicaInfoAdded = false;
    boolean duplicateFound = false;
    if (diskInfo != null) {
      // add replica to an existing disk (need to sort replicas by partition id)
      diskInfoToAdd = diskInfo;
      String replicasStr = diskInfo.get(REPLICAS_STR);
      String[] replicaInfos = replicasStr.split(REPLICAS_DELIM_STR);
      StringBuilder replicasStrBuilder = new StringBuilder();
      long idToAdd = Long.parseLong(partitionName);
      for (String replicaInfo : replicaInfos) {
        String[] infos = replicaInfo.split(REPLICAS_STR_SEPARATOR);
        long currentId = Long.parseLong(infos[0]);
        if (currentId == idToAdd) {
          logger.info("Partition {} is already on instance {}, skipping adding it into InstanceConfig in Helix.",
              partitionName, instanceName);
          duplicateFound = true;
          break;
        } else if (currentId < idToAdd || newReplicaInfoAdded) {
          replicasStrBuilder.append(replicaInfo).append(REPLICAS_DELIM_STR);
        } else {
          // newReplicaInfo already contains delimiter, no need to append REPLICAS_DELIM_STR
          replicasStrBuilder.append(newReplicaInfo);
          replicasStrBuilder.append(replicaInfo).append(REPLICAS_DELIM_STR);
          newReplicaInfoAdded = true;
        }
      }
      if (!duplicateFound && !newReplicaInfoAdded) {
        // this means new replica id is larger than all existing replicas' ids
        replicasStrBuilder.append(newReplicaInfo);
        newReplicaInfoAdded = true;
      }
      if (newReplicaInfoAdded) {
        diskInfoToAdd.put(REPLICAS_STR, replicasStrBuilder.toString());
      }
    } else {
      // add replica onto a brand new disk
      diskInfoToAdd = new HashMap<>();
      logger.info("Adding info of new replica {} to the new disk {}", replicaId.getPartitionId().toPathString(),
          replicaId.getDiskId());
      diskInfoToAdd.put(DISK_CAPACITY_STR, Long.toString(replicaId.getDiskId().getRawCapacityInBytes()));
      diskInfoToAdd.put(DISK_STATE, AVAILABLE_STR);
      diskInfoToAdd.put(REPLICAS_STR, newReplicaInfo);
      newReplicaInfoAdded = true;
    }
    if (newReplicaInfoAdded) {
      // we update InstanceConfig only when new replica info is added (skip updating if replica is already present)
      instanceConfig.getRecord().setMapField(replicaId.getMountPath(), diskInfoToAdd);
      logger.info("Updating config: {} in Helix by adding partition {}", instanceConfig, partitionName);
      additionResult = helixAdmin.setInstanceConfig(clusterName, instanceName, instanceConfig);
    }
    return additionResult;
  }

  /**
   * Remove old/existing replica info from {@link InstanceConfig} that associates with current data node.
   * @param replicaId the {@link ReplicaId} whose info should be removed.
   * @return {@code true} replica info is successfully removed. {@code false} otherwise.
   */
  private boolean removeOldReplicaInfo(ReplicaId replicaId) {
    boolean removalResult = true;
    boolean instanceConfigUpdated = false;
    boolean replicaFound;
    if (!disablePartitionsComplete) {
      // block here until there is a ZNode associated with current node has been created under /PROPERTYSTORE/AdminConfig/
      try {
        awaitDisablingPartition();
      } catch (InterruptedException e) {
        logger.error("Awaiting completion of disabling partition was interrupted. ", e);
        return false;
      }
      disablePartitionsComplete = true;
    }
    InstanceConfig instanceConfig = getInstanceConfig();
    String partitionName = replicaId.getPartitionId().toPathString();
    List<String> stoppedReplicas = instanceConfig.getRecord().getListField(STOPPED_REPLICAS_STR);
    List<String> sealedReplicas = instanceConfig.getRecord().getListField(SEALED_STR);
    stoppedReplicas = stoppedReplicas == null ? new ArrayList<>() : stoppedReplicas;
    sealedReplicas = sealedReplicas == null ? new ArrayList<>() : sealedReplicas;
    if (stoppedReplicas.remove(partitionName) || sealedReplicas.remove(partitionName)) {
      logger.info("Removing partition {} from stopped and sealed list", partitionName);
      instanceConfig.getRecord().setListField(STOPPED_REPLICAS_STR, stoppedReplicas);
      instanceConfig.getRecord().setListField(SEALED_STR, sealedReplicas);
      instanceConfigUpdated = true;
    }
    Map<String, Map<String, String>> mountPathToDiskInfos = instanceConfig.getRecord().getMapFields();
    Map<String, String> diskInfo = mountPathToDiskInfos.get(replicaId.getMountPath());
    if (diskInfo != null) {
      String replicasStr = diskInfo.get(REPLICAS_STR);
      if (!replicasStr.isEmpty()) {
        List<String> replicaInfos = new ArrayList<>(Arrays.asList(replicasStr.split(REPLICAS_DELIM_STR)));
        // if any element is removed, that means old replica is found in replicasStr.
        replicaFound = replicaInfos.removeIf(info -> (info.split(REPLICAS_STR_SEPARATOR)[0]).equals(partitionName));

        // We update InstanceConfig only when replica is found in current instanceConfig. (This is to avoid unnecessary
        // notification traffic due to InstanceConfig change)
        if (replicaFound) {
          StringBuilder newReplicasStrBuilder = new StringBuilder();
          // note that old replica info has been removed from "replicaInfos"
          for (String replicaInfo : replicaInfos) {
            newReplicasStrBuilder.append(replicaInfo).append(REPLICAS_DELIM_STR);
          }
          // update diskInfo and MountPathToDisk map
          diskInfo.put(REPLICAS_STR, newReplicasStrBuilder.toString());
          // update InstanceConfig
          instanceConfig.getRecord().setMapField(replicaId.getMountPath(), diskInfo);
          instanceConfigUpdated = true;
        }
      }
    }
    if (instanceConfigUpdated) {
      logger.info("Updating config: {} in Helix by removing partition {}", instanceConfig, partitionName);
      removalResult = helixAdmin.setInstanceConfig(clusterName, instanceName, instanceConfig);
    } else {
      logger.warn("Partition {} is not found on instance {}, skipping removing it from InstanceConfig in Helix.",
          partitionName, instanceName);
    }
    return removalResult;
  }

  /**
   * Wait until disabling partition process has completed. This is to avoid race condition where server and Helix may
   * modify same InstanceConfig.
   * TODO remove this method after migrating ambry to PropertyStore (in Helix).
   * @throws InterruptedException
   */
  private void awaitDisablingPartition() throws InterruptedException {
    Properties properties = new Properties();
    properties.setProperty("helix.property.store.root.path", "/" + clusterName + "/" + PROPERTYSTORE_STR);
    HelixPropertyStoreConfig propertyStoreConfig = new HelixPropertyStoreConfig(new VerifiableProperties(properties));
    HelixPropertyStore<ZNRecord> helixPropertyStore =
        CommonUtils.createHelixPropertyStore(zkConnectStr, propertyStoreConfig, null);
    String path = PARTITION_DISABLED_ZNODE_PATH + instanceName;
    int count = 1;
    while (helixPropertyStore.exists(path, AccessOption.PERSISTENT)) {
      // Thread.sleep() pauses the current thread but does not release any locks
      Thread.sleep(clusterMapConfig.clustermapRetryDisablePartitionCompletionBackoffMs);
      logger.info("{} th attempt on checking the completion of disabling partition.", ++count);
    }
    helixPropertyStore.stop();
  }

  /**
   * Register {@link HelixHealthReportAggregatorTask}s for appropriate {@link AmbryHealthReport}s.
   * @param engine the {@link StateMachineEngine} to register the task state model.
   * @param healthReports the {@link List} of {@link AmbryHealthReport}s that may require the registration of
   * corresponding {@link HelixHealthReportAggregatorTask}s.
   * @param callback a callback which will be invoked when the aggregation report has been generated successfully.
   */
  private void registerHealthReportTasks(StateMachineEngine engine, List<AmbryHealthReport> healthReports,
      Callback<StatsSnapshot> callback) {
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
                    healthReport.getReportName(), healthReport.getStatsFieldName(), healthReport.getStatsReportType(),
                    callback, clusterMapConfig);
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
   * @return {@link InstanceConfig} of current participant (The method also checks the existence of InstanceConfig).
   */
  private InstanceConfig getInstanceConfig() {
    InstanceConfig instanceConfig = helixAdmin.getInstanceConfig(clusterName, instanceName);
    if (instanceConfig == null) {
      throw new IllegalStateException(
          "No instance config found for cluster: \"" + clusterName + "\", instance: \"" + instanceName + "\"");
    }
    return instanceConfig;
  }

  /**
   * Set the list of sealed replicas in the HelixAdmin. This method is called only after the helixAdministrationLock
   * is taken.
   * @param sealedReplicas list of sealed replicas to be set in the HelixAdmin
   * @return whether the operation succeeded or not
   */
  private boolean setSealedReplicas(List<String> sealedReplicas) {
    InstanceConfig instanceConfig = getInstanceConfig();
    logger.trace("Setting InstanceConfig with list of sealed replicas: {}", sealedReplicas.toArray());
    instanceConfig.getRecord().setListField(SEALED_STR, sealedReplicas);
    return helixAdmin.setInstanceConfig(clusterName, instanceName, instanceConfig);
  }

  /**
   * Set the list of stopped replicas in the HelixAdmin. This method is called only after the helixAdministrationLock
   * is taken.
   * @param stoppedReplicas list of stopped replicas to be set in the HelixAdmin
   * @return whether the operation succeeded or not
   */
  boolean setStoppedReplicas(List<String> stoppedReplicas) {
    InstanceConfig instanceConfig = getInstanceConfig();
    logger.trace("Setting InstanceConfig with list of stopped replicas: {}", stoppedReplicas.toArray());
    instanceConfig.getRecord().setListField(STOPPED_REPLICAS_STR, stoppedReplicas);
    return helixAdmin.setInstanceConfig(clusterName, instanceName, instanceConfig);
  }

  @Override
  public void onPartitionBecomeBootstrapFromOffline(String partitionName) {
    try {
      // 1. take actions in storage manager (add new replica if necessary)
      PartitionStateChangeListener storageManagerListener =
          partitionStateChangeListeners.get(StateModelListenerType.StorageManagerListener);
      if (storageManagerListener != null) {
        storageManagerListener.onPartitionBecomeBootstrapFromOffline(partitionName);
      }
      // 2. take actions in replication manager (add new replica if necessary)
      PartitionStateChangeListener replicationManagerListener =
          partitionStateChangeListeners.get(StateModelListenerType.ReplicationManagerListener);
      if (replicationManagerListener != null) {
        replicationManagerListener.onPartitionBecomeBootstrapFromOffline(partitionName);
      }
      // 3. take actions in stats manager (add new replica if necessary)
      PartitionStateChangeListener statsManagerListener =
          partitionStateChangeListeners.get(StateModelListenerType.StatsManagerListener);
      if (statsManagerListener != null) {
        statsManagerListener.onPartitionBecomeBootstrapFromOffline(partitionName);
      }
    } catch (Exception e) {
      localPartitionAndState.put(partitionName, ReplicaState.ERROR);
      throw e;
    }
    localPartitionAndState.put(partitionName, ReplicaState.BOOTSTRAP);
  }

  @Override
  public void onPartitionBecomeStandbyFromBootstrap(String partitionName) {
    PartitionStateChangeListener replicationManagerListener =
        partitionStateChangeListeners.get(StateModelListenerType.ReplicationManagerListener);
    try {
      if (replicationManagerListener != null) {

        replicationManagerListener.onPartitionBecomeStandbyFromBootstrap(partitionName);
        // after bootstrap is initiated in ReplicationManager, transition is blocked here and wait until local replica has
        // caught up with enough peer replicas.
        replicaSyncUpManager.waitBootstrapCompleted(partitionName);
      }
    } catch (InterruptedException e) {
      logger.error("Bootstrap was interrupted on partition {}", partitionName);
      localPartitionAndState.put(partitionName, ReplicaState.ERROR);
      throw new StateTransitionException("Bootstrap failed or was interrupted", BootstrapFailure);
    } catch (StateTransitionException e) {
      logger.error("Bootstrap didn't complete on partition {}", partitionName, e);
      localPartitionAndState.put(partitionName, ReplicaState.ERROR);
      throw e;
    }
    localPartitionAndState.put(partitionName, ReplicaState.STANDBY);
  }

  @Override
  public void onPartitionBecomeLeaderFromStandby(String partitionName) {
    try {
      PartitionStateChangeListener cloudToStoreReplicationListener =
          partitionStateChangeListeners.get(StateModelListenerType.CloudToStoreReplicationManagerListener);
      if (cloudToStoreReplicationListener != null) {
        cloudToStoreReplicationListener.onPartitionBecomeLeaderFromStandby(partitionName);
      }
      PartitionStateChangeListener replicationManagerListener =
          partitionStateChangeListeners.get(StateModelListenerType.ReplicationManagerListener);
      if (replicationManagerListener != null) {
        replicationManagerListener.onPartitionBecomeLeaderFromStandby(partitionName);
      }
    } catch (Exception e) {
      localPartitionAndState.put(partitionName, ReplicaState.ERROR);
      throw e;
    }
    localPartitionAndState.put(partitionName, ReplicaState.LEADER);
  }

  @Override
  public void onPartitionBecomeStandbyFromLeader(String partitionName) {
    try {
      PartitionStateChangeListener cloudToStoreReplicationListener =
          partitionStateChangeListeners.get(StateModelListenerType.CloudToStoreReplicationManagerListener);
      if (cloudToStoreReplicationListener != null) {
        cloudToStoreReplicationListener.onPartitionBecomeStandbyFromLeader(partitionName);
      }
      PartitionStateChangeListener replicationManagerListener =
          partitionStateChangeListeners.get(StateModelListenerType.ReplicationManagerListener);
      if (replicationManagerListener != null) {
        replicationManagerListener.onPartitionBecomeStandbyFromLeader(partitionName);
      }
    } catch (Exception e) {
      localPartitionAndState.put(partitionName, ReplicaState.ERROR);
      throw e;
    }
    localPartitionAndState.put(partitionName, ReplicaState.STANDBY);
  }

  @Override
  public void onPartitionBecomeInactiveFromStandby(String partitionName) {
    // 1. storage manager marks store local state as INACTIVE and disables compaction on this partition
    PartitionStateChangeListener storageManagerListener =
        partitionStateChangeListeners.get(StateModelListenerType.StorageManagerListener);
    if (storageManagerListener != null) {
      try {
        storageManagerListener.onPartitionBecomeInactiveFromStandby(partitionName);
      } catch (Exception e) {
        localPartitionAndState.put(partitionName, ReplicaState.ERROR);
        throw e;
      }
    }
    // 2. replication manager initiates deactivation
    PartitionStateChangeListener replicationManagerListener =
        partitionStateChangeListeners.get(StateModelListenerType.ReplicationManagerListener);
    try {
      if (replicationManagerListener != null) {

        replicationManagerListener.onPartitionBecomeInactiveFromStandby(partitionName);
        // after deactivation is initiated in ReplicationManager, transition is blocked here and wait until enough peer
        // replicas have caught up with last PUT in local store.
        // TODO considering moving wait deactivation logic into replication manager listener
        replicaSyncUpManager.waitDeactivationCompleted(partitionName);
      }
    } catch (InterruptedException e) {
      logger.error("Deactivation was interrupted on partition {}", partitionName);
      localPartitionAndState.put(partitionName, ReplicaState.ERROR);
      throw new StateTransitionException("Deactivation failed or was interrupted", DeactivationFailure);
    } catch (StateTransitionException e) {
      logger.error("Deactivation didn't complete on partition {}", partitionName, e);
      localPartitionAndState.put(partitionName, ReplicaState.ERROR);
      throw e;
    }
    localPartitionAndState.put(partitionName, ReplicaState.INACTIVE);
  }

  @Override
  public void onPartitionBecomeOfflineFromInactive(String partitionName) {
    PartitionStateChangeListener replicationManagerListener =
        partitionStateChangeListeners.get(StateModelListenerType.ReplicationManagerListener);
    try {
      if (replicationManagerListener != null) {
        // 1. take actions in replication manager
        //    (1) set local store state to OFFLINE
        //    (2) initiate disconnection in ReplicaSyncUpManager
        replicationManagerListener.onPartitionBecomeOfflineFromInactive(partitionName);
        // 2. wait until peer replicas have caught up with local replica
        // TODO considering moving wait disconnection logic into replication manager listener
        replicaSyncUpManager.waitDisconnectionCompleted(partitionName);
      }
      // 3. take actions in storage manager (stop the store and update instanceConfig)
      PartitionStateChangeListener storageManagerListener =
          partitionStateChangeListeners.get(StateModelListenerType.StorageManagerListener);
      if (storageManagerListener != null) {
        storageManagerListener.onPartitionBecomeOfflineFromInactive(partitionName);
      }
    } catch (InterruptedException e) {
      logger.error("Disconnection was interrupted on partition {}", partitionName);
      localPartitionAndState.put(partitionName, ReplicaState.ERROR);
      throw new StateTransitionException("Disconnection failed or was interrupted", DisconnectionFailure);
    } catch (StateTransitionException e) {
      logger.error("Exception occurred during Inactive-To-Offline transition ", e);
      localPartitionAndState.put(partitionName, ReplicaState.ERROR);
      throw e;
    }
    localPartitionAndState.put(partitionName, ReplicaState.OFFLINE);
  }

  @Override
  public void onPartitionBecomeDroppedFromOffline(String partitionName) {
    // remove old replica from StorageManager and delete store directory (this also includes recover from decommission
    // failure and remove old replica from replication/stats manager)
    PartitionStateChangeListener storageManagerListener =
        partitionStateChangeListeners.get(StateModelListenerType.StorageManagerListener);
    try {
      if (storageManagerListener != null) {
        storageManagerListener.onPartitionBecomeDroppedFromOffline(partitionName);
      }
    } catch (Exception e) {
      localPartitionAndState.put(partitionName, ReplicaState.ERROR);
      throw e;
    }
    localPartitionAndState.remove(partitionName);
    participantMetrics.partitionDroppedCount.inc();
  }

  @Override
  public void onPartitionBecomeDroppedFromError(String partitionName) {
    localPartitionAndState.remove(partitionName);
    participantMetrics.partitionDroppedCount.inc();
  }

  @Override
  public void onPartitionBecomeOfflineFromError(String partitionName) {
    localPartitionAndState.put(partitionName, ReplicaState.OFFLINE);
  }

  @Override
  public void onReset(String partitionName) {
    localPartitionAndState.put(partitionName, ReplicaState.OFFLINE);
  }
}
