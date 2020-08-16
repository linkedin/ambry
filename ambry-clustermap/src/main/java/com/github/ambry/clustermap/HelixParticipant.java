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
  private final String instanceName;
  private final ReplicaSyncUpManager replicaSyncUpManager;
  private final DataNodeConfigSource dataNodeConfigSource;
  private final HelixAdmin helixAdmin;
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
    manager = helixFactory.getZKHelixManager(clusterName, instanceName, InstanceType.PARTICIPANT, zkConnectStr);
    replicaSyncUpManager = new AmbryReplicaSyncUpManager(clusterMapConfig);
    partitionStateChangeListeners = new HashMap<>();
    try {
      // since reading/writing from zookeeper does not rely on HelixManager being a PARTICIPANT, we can share the
      // SPECTATOR instance that is also used by HelixClusterManager. This avoids the need to start participating in a
      // cluster before reading/writing from zookeeper.
      HelixManager spectatorManager =
          helixFactory.getZkHelixManagerAndConnect(clusterName, instanceName, InstanceType.SPECTATOR, zkConnectStr);
      helixAdmin = spectatorManager.getClusterManagmentTool();
      dataNodeConfigSource =
          getDataNodeConfigSource(clusterMapConfig, spectatorManager, new DataNodeConfigSourceMetrics(metricRegistry));
    } catch (Exception exception) {
      throw new IllegalStateException("Error setting up administration facilities", exception);
    }
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
      // register server as a participant
      manager.connect();
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
      DataNodeConfig config = getDataNodeConfig();
      String partitionId = replicaId.getPartitionId().toPathString();
      boolean success = true;
      if (!isSealed && config.getSealedReplicas().remove(partitionId)) {
        logger.trace("Removing the partition {} from sealedReplicas list", partitionId);
        success = dataNodeConfigSource.set(config);
      } else if (isSealed && config.getSealedReplicas().add(partitionId)) {
        logger.trace("Adding the partition {} to sealedReplicas list", partitionId);
        success = dataNodeConfigSource.set(config);
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
      logger.info("Getting stopped replicas from Helix");
      DataNodeConfig config = getDataNodeConfig();
      boolean stoppedSetUpdated = markStop ? config.getStoppedReplicas().addAll(replicasToUpdate)
          : config.getStoppedReplicas().removeAll(replicasToUpdate);
      if (stoppedSetUpdated) {
        logger.info("Updating the stopped list in Helix to {}", config.getStoppedReplicas());
        setStoppedResult = dataNodeConfigSource.set(config);
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
    // TODO refactor these getter methods to return set instead of list.
    return new ArrayList<>(getDataNodeConfig().getSealedReplicas());
  }

  /**
   * @return list of stopped replicas from HelixAdmin
   */
  @Override
  public List<String> getStoppedReplicas() {
    return new ArrayList<>(getDataNodeConfig().getStoppedReplicas());
  }

  @Override
  public List<String> getDisabledReplicas() {
    return new ArrayList<>(getDataNodeConfig().getDisabledReplicas());
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
      String partitionName = replicaId.getPartitionId().toPathString();

      // 1. update disabled replica list in DataNodeConfig. This modifies ListFields only
      boolean dataNodeConfigChanged = false;
      DataNodeConfig dataNodeConfig = getDataNodeConfig();
      if (!disable && dataNodeConfig.getDisabledReplicas().remove(partitionName)) {
        logger.info("Removing the partition {} from disabledReplicas list", partitionName);
        dataNodeConfigChanged = true;
      } else if (disable && dataNodeConfig.getDisabledReplicas().add(partitionName)) {
        logger.info("Adding the partition {} to disabledReplicas list", partitionName);
        dataNodeConfigChanged = true;
      }
      if (dataNodeConfigChanged) {
        logger.info("Setting config with list of disabled replicas: {}", dataNodeConfig.getDisabledReplicas());
        if (!dataNodeConfigSource.set(dataNodeConfig)) {
          participantMetrics.setReplicaDisabledStateErrorCount.inc();
          logger.warn("setReplicaDisabledState() failed DataNodeConfig update");
        }

        // 2. If the DataNodeConfig was changed, invoke Helix native method to enable/disable partition on local node,
        //    this will trigger subsequent state transition on given replica. This method modifies MapFields in
        //    InstanceConfig.
        InstanceConfig instanceConfig = getInstanceConfig();
        String resourceNameForPartition = getResourceNameOfPartition(helixAdmin, clusterName, partitionName);
        logger.info("{} replica {} on current node", disable ? "Disabling" : "Enabling", partitionName);
        instanceConfig.setInstanceEnabledForPartition(resourceNameForPartition, partitionName, !disable);
        if (!helixAdmin.setInstanceConfig(clusterName, instanceName, instanceConfig)) {
          participantMetrics.setReplicaDisabledStateErrorCount.inc();
          logger.warn("setReplicaDisabledState() failed InstanceConfig update");
        }
      }
      logger.info("Disabled state of partition {} is updated", partitionName);
    }
  }

  @Override
  public boolean resetPartitionState(String partitionName) {
    boolean result = true;
    try {
      String resourceName = getResourceNameOfPartition(helixAdmin, clusterName, partitionName);
      helixAdmin.resetPartition(clusterName, instanceName, resourceName, Collections.singletonList(partitionName));
    } catch (Exception e) {
      logger.error("Exception occurred when resetting partition " + partitionName, e);
      result = false;
    }
    return result;
  }

  @Override
  public boolean supportsStateChanges() {
    return true;
  }

  /**
   * Exposed for testing
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
   * Add new replica info into {@link DataNodeConfig} of current data node.
   * @param replicaId new replica whose info should be added into {@link DataNodeConfig}.
   * @return {@code true} replica info is successfully added. {@code false} otherwise.
   */
  private boolean addNewReplicaInfo(ReplicaId replicaId) {
    boolean additionResult = true;
    DataNodeConfig dataNodeConfig = getDataNodeConfig();
    String partitionName = replicaId.getPartitionId().toPathString();
    DataNodeConfig.ReplicaConfig replicaConfigToAdd = new DataNodeConfig.ReplicaConfig(replicaId.getCapacityInBytes(),
        replicaId.getPartitionId().getPartitionClass());
    DataNodeConfig.DiskConfig diskConfig = dataNodeConfig.getDiskConfigs().get(replicaId.getMountPath());
    boolean newReplicaInfoAdded = false;
    if (diskConfig != null) {
      // add replica to an existing disk
      if (diskConfig.getReplicaConfigs().containsKey(partitionName)) {
        logger.info("Partition {} is already on instance {}, skipping adding it into configs in Helix.", partitionName,
            instanceName);
      } else {
        diskConfig.getReplicaConfigs().put(partitionName, replicaConfigToAdd);
        newReplicaInfoAdded = true;
      }
    } else {
      // add replica onto a brand new disk
      logger.info("Adding info of new replica {} to the new disk {}", replicaId.getPartitionId().toPathString(),
          replicaId.getDiskId());
      DataNodeConfig.DiskConfig diskConfigToAdd =
          new DataNodeConfig.DiskConfig(HardwareState.AVAILABLE, replicaId.getDiskId().getRawCapacityInBytes());
      diskConfigToAdd.getReplicaConfigs().put(partitionName, replicaConfigToAdd);
      dataNodeConfig.getDiskConfigs().put(replicaId.getMountPath(), diskConfigToAdd);
      newReplicaInfoAdded = true;
    }
    if (newReplicaInfoAdded) {
      logger.info("Updating config: {} in Helix by adding partition {}", dataNodeConfig, partitionName);
      additionResult = dataNodeConfigSource.set(dataNodeConfig);
    }
    return additionResult;
  }

  /**
   * Remove old/existing replica info from {@link DataNodeConfig} that associates with current data node.
   * @param replicaId the {@link ReplicaId} whose info should be removed.
   * @return {@code true} replica info is successfully removed. {@code false} otherwise.
   */
  private boolean removeOldReplicaInfo(ReplicaId replicaId) {
    boolean removalResult = true;
    boolean dataNodeConfigUpdated = false;
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
    DataNodeConfig dataNodeConfig = getDataNodeConfig();
    String partitionName = replicaId.getPartitionId().toPathString();
    boolean removedFromStopped = dataNodeConfig.getStoppedReplicas().remove(partitionName);
    boolean removedFromSealed = dataNodeConfig.getSealedReplicas().remove(partitionName);
    if (removedFromStopped || removedFromSealed) {
      logger.info("Removing partition {} from stopped and sealed list", partitionName);
      dataNodeConfigUpdated = true;
    }
    DataNodeConfig.DiskConfig diskConfig = dataNodeConfig.getDiskConfigs().get(replicaId.getMountPath());
    if (diskConfig != null) {
      dataNodeConfigUpdated = diskConfig.getReplicaConfigs().remove(partitionName) != null;
    }
    if (dataNodeConfigUpdated) {
      logger.info("Updating config: {} in Helix by removing partition {}", dataNodeConfig, partitionName);
      removalResult = dataNodeConfigSource.set(dataNodeConfig);
    } else {
      logger.warn("Partition {} is not found on instance {}, skipping removing it from config in Helix.", partitionName,
          instanceName);
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
   * @return {@link DataNodeConfig} of current participant (The method also checks the existence of the config).
   */
  private DataNodeConfig getDataNodeConfig() {
    DataNodeConfig dataNodeConfig = dataNodeConfigSource.get(instanceName);
    if (dataNodeConfig == null) {
      throw new IllegalStateException(
          "No config found for cluster: \"" + clusterName + "\", instance: \"" + instanceName + "\"");
    }
    return dataNodeConfig;
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
