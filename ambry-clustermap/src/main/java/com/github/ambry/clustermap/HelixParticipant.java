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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.accountstats.AccountStatsStore;
import com.github.ambry.commons.Callback;
import com.github.ambry.commons.CommonUtils;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.HelixPropertyStoreConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.server.AmbryStatsReport;
import com.github.ambry.server.storagestats.AggregatedAccountStorageStats;
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
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.helix.AccessOption;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.InstanceType;
import org.apache.helix.PropertyKey;
import org.apache.helix.lock.LockScope;
import org.apache.helix.lock.helix.HelixLockScope;
import org.apache.helix.lock.helix.ZKDistributedNonblockingLock;
import org.apache.helix.model.ControllerHistory;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.store.HelixPropertyStore;
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
  public static final String DISK_KEY = "DISK";
  final HelixParticipantMetrics participantMetrics;
  private final HelixClusterManager clusterManager;
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
  private final MetricRegistry metricRegistry;
  private volatile boolean disablePartitionsComplete = false;
  private volatile boolean inMaintenanceMode = false;
  private volatile String maintenanceModeReason = null;
  final Map<StateModelListenerType, PartitionStateChangeListener> partitionStateChangeListeners;
  private static final ObjectMapper objectMapper = new ObjectMapper();

  private static final Logger logger = LoggerFactory.getLogger(HelixParticipant.class);

  /**
   * Instantiate a HelixParticipant.
   * @param clusterMapConfig the {@link ClusterMapConfig} associated with this participant.
   * @param helixFactory the {@link HelixFactory} to use to get the {@link HelixManager}.
   * @param metricRegistry the {@link MetricRegistry} to instantiate {@link HelixParticipantMetrics}.
   * @param zkConnectStr the address identifying the zk service which this participant interacts with.
   * @param isSoleParticipant whether this is the sole participant on current node.
   */
  public HelixParticipant(HelixClusterManager clusterManager, ClusterMapConfig clusterMapConfig,
      HelixFactory helixFactory, MetricRegistry metricRegistry, String zkConnectStr, boolean isSoleParticipant) {
    this.clusterMapConfig = clusterMapConfig;
    this.clusterManager = clusterManager;
    this.zkConnectStr = zkConnectStr;
    this.metricRegistry = metricRegistry;
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
      dataNodeConfigSource = helixFactory.getDataNodeConfigSource(clusterMapConfig, zkConnectStr,
          new DataNodeConfigSourceMetrics(metricRegistry));
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
   * @param ambryStatsReports {@link List} of {@link AmbryStatsReport} to be registered to the participant.
   * @param accountStatsStore the {@link AccountStatsStore} to retrieve and store container stats.
   * @param callback a callback which will be invoked when the aggregation report has been generated successfully.
   * @throws IOException if there is an error connecting to the Helix cluster.
   */
  @Override
  public void participate(List<AmbryStatsReport> ambryStatsReports, AccountStatsStore accountStatsStore,
      Callback<AggregatedAccountStorageStats> callback) throws IOException {
    logger.info("Initiating the participation. The specified state model is {}",
        clusterMapConfig.clustermapStateModelDefinition);
    StateMachineEngine stateMachineEngine = manager.getStateMachineEngine();
    stateMachineEngine.registerStateModelFactory(clusterMapConfig.clustermapStateModelDefinition,
        new AmbryStateModelFactory(clusterMapConfig, this, clusterManager));
    registerTasks(stateMachineEngine, ambryStatsReports, accountStatsStore, callback);
    try {
      // register server as a participant
      manager.connect();
    } catch (Exception e) {
      throw new IOException("Exception while connecting to the Helix manager", e);
    }
    logger.info("Completed participation in cluster {} at {}", clusterName, zkConnectStr);
  }

  @Override
  public void registerPartitionStateChangeListener(StateModelListenerType listenerType,
      PartitionStateChangeListener partitionStateChangeListener) {
    partitionStateChangeListeners.put(listenerType, partitionStateChangeListener);
  }

  @Override
  public boolean setReplicaSealedState(ReplicaId replicaId, ReplicaSealStatus replicaSealStatus) {
    if (!(replicaId instanceof AmbryReplica)) {
      throw new IllegalArgumentException(
          "HelixParticipant only works with the AmbryReplica implementation of ReplicaId");
    }
    synchronized (helixAdministrationLock) {
      DataNodeConfig config = getDataNodeConfig();
      String partitionId = replicaId.getPartitionId().toPathString();
      boolean success = false;
      boolean configChanged;
      switch (replicaSealStatus) {
        case NOT_SEALED:
          configChanged = config.getSealedReplicas().remove(partitionId);
          configChanged = config.getPartiallySealedReplicas().remove(partitionId) || configChanged;
          if (configChanged) {
            logger.trace("Removing partition {} from sealed lists to mark it as NOT_SEALED", partitionId);
            success = dataNodeConfigSource.set(config);
          }
          break;
        case PARTIALLY_SEALED:
          configChanged = config.getPartiallySealedReplicas().add(partitionId);
          configChanged = config.getSealedReplicas().remove(partitionId) || configChanged;
          if (configChanged) {
            logger.trace("Adding the partition {} to partially sealed list to mark it as PARTIALLY_SEALED",
                partitionId);
            success = dataNodeConfigSource.set(config);
          }
          break;
        case SEALED:
          configChanged = config.getSealedReplicas().add(partitionId);
          configChanged = config.getPartiallySealedReplicas().remove(partitionId) || configChanged;
          if (configChanged) {
            logger.trace("Adding the partition {} to sealedReplicas list", partitionId);
            success = dataNodeConfigSource.set(config);
          }
          break;
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

  @Override
  public List<String> getPartiallySealedReplicas() {
    return new ArrayList<>(getDataNodeConfig().getPartiallySealedReplicas());
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

  @Override
  public boolean removeReplicasFromDataNode(List<ReplicaId> replicaIds) {
    if (!clusterMapConfig.clustermapUpdateDatanodeInfo) {
      return false;
    }
    if (replicaIds == null) {
      throw new IllegalArgumentException("Invalid replica list to remove");
    }
    synchronized (helixAdministrationLock) {
      DataNodeConfig dataNodeConfig = getDataNodeConfig();
      boolean dataNodeConfigUpdated = false;
      boolean removalResult = true;
      List<String> partitionNames = new ArrayList<>();
      for (ReplicaId replicaId : replicaIds) {
        String partitionName = replicaId.getPartitionId().toPathString();
        boolean removedFromStopped = dataNodeConfig.getStoppedReplicas().remove(partitionName);
        boolean removedFromSealed = dataNodeConfig.getSealedReplicas().remove(partitionName);
        boolean update = false;
        if (removedFromStopped || removedFromSealed) {
          logger.info("Removing partition {} from stopped and sealed list", partitionName);
          dataNodeConfigUpdated = true;
          update = true;
        }
        DataNodeConfig.DiskConfig diskConfig = dataNodeConfig.getDiskConfigs().get(replicaId.getMountPath());
        if (diskConfig != null && diskConfig.getReplicaConfigs().remove(partitionName) != null) {
          logger.info("Removing partition {} from disk {}' config list", partitionName, replicaId.getMountPath());
          dataNodeConfigUpdated = true;
          update = true;
        }
        if (update) {
          partitionNames.add(partitionName);
        }
      }
      if (dataNodeConfigUpdated) {
        logger.info("Updating config in Helix by removing partitions {}", partitionNames);
        removalResult = dataNodeConfigSource.set(dataNodeConfig);
      } else {
        logger.warn("Partitions {} is not found on instance {}, skipping removing them from config in Helix.",
            partitionNames, instanceName);
      }
      return removalResult;
    }
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
    setPartitionDisabledState(replicaId.getPartitionId().toPathString(), disable);
  }

  @Override
  public boolean resetPartitionState(String partitionName) {
    boolean result = true;
    try {
      String resourceName = getResourceNameOfPartition(helixAdmin, clusterName, partitionName);
      if (resourceName == null) {
        logger.error("Can't find resource for partition {} when resetting partition state", partitionName);
        return false;
      }
      helixAdmin.resetPartition(clusterName, instanceName, resourceName, Collections.singletonList(partitionName));
    } catch (Exception e) {
      logger.error("Exception occurred when resetting partition " + partitionName, e);
      result = false;
    }
    return result;
  }

  @Override
  public boolean resetPartitionState(List<String> partitionNames) {
    boolean result = true;
    List<String> succeededPartitions = new ArrayList<>();
    try {
      Map<String, List<String>> resourceNameToPartitionNames = new HashMap<>();
      for (String partitionName : partitionNames) {
        String resourceName = getResourceNameOfPartition(helixAdmin, clusterName, partitionName);
        if (resourceName == null) {
          logger.error("Can't find resource for partition {} when resetting the list of partition states",
              partitionName);
          return false;
        }
        resourceNameToPartitionNames.computeIfAbsent(resourceName, k -> new ArrayList()).add(partitionName);
      }
      for (Map.Entry<String, List<String>> entry : resourceNameToPartitionNames.entrySet()) {
        String resourceName = entry.getKey();
        List<String> partitions = entry.getValue();
        helixAdmin.resetPartition(clusterName, instanceName, resourceName, partitions);
        succeededPartitions.addAll(partitions);
      }
    } catch (Exception e) {
      logger.error("Exception occurred when resetting partition {}, succeeded partitions are {}", partitionNames,
          succeededPartitions, e);
      result = false;
    }
    return result;
  }

  @Override
  public boolean setDisksState(List<DiskId> diskIds, HardwareState state) {
    if (diskIds == null || diskIds.isEmpty()) {
      // when calling this method, we know that some disks are bad, so empty list is not allowed.
      throw new IllegalArgumentException("List of disk is empty when set disks state");
    }
    for (DiskId diskId : diskIds) {
      if (!(diskId instanceof AmbryDisk)) {
        throw new IllegalArgumentException(
            "HelixParticipant only works with the AmbryDisk implementation of DiskId: " + diskIds);
      }
    }
    synchronized (helixAdministrationLock) {
      // update availability in DataNodeConfig.
      DataNodeConfig dataNodeConfig = getDataNodeConfig();
      boolean success = true;
      boolean dataNodeConfigChanged = false;
      for (DiskId diskId : diskIds) {
        DataNodeConfig.DiskConfig diskConfig = dataNodeConfig.getDiskConfigs().get(diskId.getMountPath());
        if (diskConfig == null) {
          throw new IllegalArgumentException(
              "Disk " + diskId.getMountPath() + " can't be found in the failed DataNodeConfig");
        }
        if (diskConfig.getState() != state) {
          logger.info("Setting disk {} state from {} to {}", diskId.getMountPath(), diskConfig.getState(), state);
          dataNodeConfigChanged = true;
          DataNodeConfig.DiskConfig newDiskConfig =
              new DataNodeConfig.DiskConfig(state, diskConfig.getDiskCapacityInBytes());
          newDiskConfig.getReplicaConfigs().putAll(diskConfig.getReplicaConfigs());
          dataNodeConfig.getDiskConfigs().put(diskId.getMountPath(), newDiskConfig);
        }
      }

      if (dataNodeConfigChanged) {
        if (!dataNodeConfigSource.set(dataNodeConfig)) {
          logger.error("Setting disks {} state failed DataNodeConfig update", diskIds);
          success = false;
        }
      }
      return success;
    }
  }

  /**
   * Given a map of old disks and new disks, swaps the old disks with the new disks in the DataNodeConfig,
   * and persists the resulting changes to Helix.
   * @param newDiskMapping A map of old disks to new disks.
   * @return {@code true} if the disks order was successfully updated, {@code false} otherwise.
   */
  public boolean setDisksOrder(Map<DiskId, DiskId> newDiskMapping) {
    if (newDiskMapping == null || newDiskMapping.isEmpty()) {
      throw new IllegalArgumentException("Map of disk mappings is empty when attempting to set disks order");
    }

    // Update DataNodeConfig and save it.
    synchronized (helixAdministrationLock) {
      DataNodeConfig dataNodeConfig = getDataNodeConfig();

      // Make a copy of the disk configs to avoid accidentally overwriting state.
      Map<String, DataNodeConfig.DiskConfig> originalDiskConfigs = new HashMap<> (dataNodeConfig.getDiskConfigs());
      for (DiskId oldDisk : newDiskMapping.keySet()) {
        DiskId newDisk = newDiskMapping.get(oldDisk);

        // Confirm that both disks are present in the DataNodeConfig
        DataNodeConfig.DiskConfig oldDiskConfig = originalDiskConfigs.get(oldDisk.getMountPath());
        DataNodeConfig.DiskConfig newDiskConfig = originalDiskConfigs.get(newDisk.getMountPath());
        if (oldDiskConfig == null || newDiskConfig == null) {
          throw new IllegalArgumentException("Disk " + oldDisk.getMountPath() + " or " + newDisk.getMountPath() + " cannot be found in Helix (DataNodeConfig)");
        }

        // Swap the disks in the DataNodeConfig
        logger.info("Replacing disk {} with disk {}", oldDisk.getMountPath(), newDisk.getMountPath());
        dataNodeConfig.getDiskConfigs().put(oldDisk.getMountPath(), originalDiskConfigs.get(newDisk.getMountPath()));
      }

      // Save the updated DataNodeConfig to Helix.
      if (!dataNodeConfigSource.set(dataNodeConfig)) {
        logger.error("Setting disks order failed DataNodeConfig update");
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean supportsStateChanges() {
    return true;
  }

  @Override
  public DistributedLock getDistributedLock(String resource, String message) {
    LockScope distributedLockScope =
        new HelixLockScope(HelixLockScope.LockScopeProperty.RESOURCE, Arrays.asList(clusterName, resource));
    String userId = clusterMapConfig.clusterMapHostName + new Random().nextInt();
    ZKDistributedNonblockingLock lock = new ZKDistributedNonblockingLock(distributedLockScope, zkConnectStr,
        clusterMapConfig.clustermapDistributedLockLeaseTimeoutInMs, message, userId);
    return new DistributedLockImpl(lock);
  }

  @Override
  public boolean enterMaintenanceMode(String reason) {
    if (reason == null || reason.isEmpty()) {
      throw new IllegalArgumentException("Reason to enter maintenance mode is missing");
    }
    boolean enterMaintenanceMode = false;
    synchronized (helixAdministrationLock) {
      if (inMaintenanceMode) {
        throw new IllegalStateException("Cluster already in maintenance mode: " + maintenanceModeReason);
      }
      try {
        // First check if cluster is already in maintenance mode.
        if (helixAdmin.isInMaintenanceMode(clusterName)) {
          logger.error("Cluster already in maintenance mode, probably due to other reason");
          return false;
        }
        helixAdmin.manuallyEnableMaintenanceMode(clusterName, true, reason, null);
        inMaintenanceMode = true;
        maintenanceModeReason = reason;
        logger.info("Cluster {} enters maintenance mode because {}", clusterName, reason);
        enterMaintenanceMode = true;
      } catch (Exception e) {
        logger.error("Exception occurred when entering maintenance mode", e);
      }
    }
    return enterMaintenanceMode;
  }

  @Override
  public boolean exitMaintenanceMode() {
    boolean exitMaintenanceMode = false;
    synchronized (helixAdministrationLock) {
      if (!inMaintenanceMode) {
        throw new IllegalStateException("Cluster is not in maintenance mode");
      }
      try {
        MaintenanceRecord record = getLastMaintenanceRecord();
        if (record.reason.equals(maintenanceModeReason)) {
          helixAdmin.manuallyEnableMaintenanceMode(clusterName, false, maintenanceModeReason, null);
          logger.info("Cluster {} exits maintenance mode: {}", clusterName, maintenanceModeReason);
        } else {
          logger.error("Maintenance record is not expected, operation type: {}, reason: {}", record.operationType,
              record.reason);
        }
        inMaintenanceMode = false;
        maintenanceModeReason = null;
        exitMaintenanceMode = true;
      } catch (Exception e) {
        logger.error("Exception occurred when entering maintenance mode", e);
      }
    }
    return exitMaintenanceMode;
  }

  /**
   * Maintenance record. This is an internal structure in helix. It might be changed with new version of helix controller.
   */
  static class MaintenanceRecord {
    @JsonProperty("DATE")
    public String date;
    @JsonProperty("TIMESTAMP")
    public String timestamp;
    @JsonProperty("OPERATION_TYPE")
    public String operationType;
    @JsonProperty("REASON")
    public String reason;
    @JsonProperty("TRIGGERED_BY")
    public String triggeredBy;
    @JsonProperty("USER")
    public String user;
  }

  /**
   * Get the latest maintenance record. We are assuming this will only be called after cluster enters maintenance mode
   * so the maintanance record should not be empty. It will throw an exception if there is no maintenance record present
   * in helix.
   * @return The latest {@link MaintenanceRecord}.
   * @throws Exception
   */
  MaintenanceRecord getLastMaintenanceRecord() throws Exception {
    PropertyKey.Builder keyBuilder = new PropertyKey.Builder(clusterName);
    PropertyKey key = keyBuilder.controllerLeaderHistory();
    ControllerHistory controllerHistory = manager.getHelixDataAccessor().getProperty(key);
    List<String> maintenanceHistory = controllerHistory.getMaintenanceHistoryList();
    if (maintenanceHistory == null || maintenanceHistory.isEmpty()) {
      throw new IllegalStateException("Missing maintenance history");
    }
    String serializedRecord = maintenanceHistory.get(maintenanceHistory.size() - 1);
    return objectMapper.readValue(serializedRecord, MaintenanceRecord.class);
  }

  @Override
  public boolean updateDiskCapacity(int diskCapacity) {
    if (diskCapacity < 0) {
      throw new IllegalArgumentException("Disk capacity has to be positive");
    }
    boolean success = false;
    synchronized (helixAdministrationLock) {
      try {
        InstanceConfig instanceConfig = helixAdmin.getInstanceConfig(clusterName, instanceName);
        Map<String, Integer> capacityMap = new HashMap<>(instanceConfig.getInstanceCapacityMap());
        // If the capacity is already the target value, then just return success
        if (capacityMap.getOrDefault(DISK_KEY, -1) != diskCapacity) {
          capacityMap.put(DISK_KEY, diskCapacity);
          instanceConfig.setInstanceCapacityMap(capacityMap);
          helixAdmin.setInstanceConfig(clusterName, instanceName, instanceConfig);
          participantMetrics.updateDiskCapacityCounter.inc();
        }
        success = true;
      } catch (Exception e) {
        logger.error("Failed to update disk capacity: {}", diskCapacity, e);
      }
    }
    return success;
  }

  /**
   * A zookeeper based implementation for distributed lock.
   */
  static class DistributedLockImpl implements DistributedLock {
    private final ZKDistributedNonblockingLock lock;

    DistributedLockImpl(ZKDistributedNonblockingLock lock) {
      this.lock = lock;
    }

    @Override
    public boolean tryLock() {
      return lock.tryLock();
    }

    @Override
    public void unlock() {
      lock.unlock();
    }

    @Override
    public void close() {
      lock.close();
    }
  }

  /**
   * Exposed for testing
   * @return {@link HelixAdmin} that manages current data node.
   */
  public HelixAdmin getHelixAdmin() {
    return helixAdmin;
  }

  /**
   * Expose for testing
   * @return {@link HelixManager} tha manage current state model.
   */
  public HelixManager getHelixManager() {
    return manager;
  }

  /**
   * Mark disablePartitionsComplete = true, this is exposed for testing only.
   */
  protected void markDisablePartitionComplete() {
    disablePartitionsComplete = true;
  }

  /**
   * Disable/enable partition on local node. This method will update both InstanceConfig and DataNodeConfig in PropertyStore.
   * @param partitionName name of partition on local node
   * @param disable if {@code true}, disable given partition on current node. {@code false} otherwise.
   */
  protected void setPartitionDisabledState(String partitionName, boolean disable) {
    synchronized (helixAdministrationLock) {
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
      logger.info("Updating config in Helix by adding partition {}", partitionName);
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
      logger.info("Updating config in Helix by removing partition {}", partitionName);
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
   * This method will register Helix Tasks
   * Register aggregation tasks for appropriate {@link AmbryStatsReport}s and {@link PropertyStoreCleanUpTask}.
   * @param engine the {@link StateMachineEngine} to register the task state model.
   * @param statsReports the {@link List} of {@link AmbryStatsReport}s that may require the registration of
   * corresponding {@link MySqlReportAggregatorTask}s.
   * @param accountStatsStore the {@link AccountStatsStore} to retrieve and store container stats.
   * @param callback a callback which will be invoked when the aggregation report has been generated successfully.
   */
  private void registerTasks(StateMachineEngine engine, List<AmbryStatsReport> statsReports,
      AccountStatsStore accountStatsStore, Callback<AggregatedAccountStorageStats> callback) {
    //Register MySqlReportAggregatorTask
    Map<String, TaskFactory> taskFactoryMap = new HashMap<>();
    for (final AmbryStatsReport statsReport : statsReports) {
      taskFactoryMap.put(
          String.format("%s_%s", MySqlReportAggregatorTask.TASK_COMMAND_PREFIX, statsReport.getReportName()),
          context -> new MySqlReportAggregatorTask(context.getManager(), statsReport.getAggregateIntervalInMinutes(),
              statsReport.getStatsReportType(), accountStatsStore, callback, clusterMapConfig, metricRegistry));
    }

    //Register PropertyStoreTask
    if(clusterMapConfig.clustermapEnablePropertyStoreCleanUpTask) {
      logger.info("Registering PropertyStoreCleanUpTask");
      taskFactoryMap.put(PropertyStoreCleanUpTask.COMMAND,
          context -> new PropertyStoreCleanUpTask(context.getManager(), dataNodeConfigSource, clusterMapConfig,
              metricRegistry));
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
      if(partitionName == "146"){
        Thread.sleep(3600000);
      } else {
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
      }
    } catch (Exception e) {
      localPartitionAndState.put(partitionName, ReplicaState.ERROR);
      try {
        throw e;
      } catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }
    }
    logger.info("Before setting partition {} to bootstrap", partitionName);
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
    logger.info("Purging disabled state of dropped replica {} from both InstanceConfig and DataNodeConfig",
        partitionName);
    setPartitionDisabledState(partitionName, false);
    localPartitionAndState.remove(partitionName);
    participantMetrics.partitionDroppedCount.inc();
  }

  @Override
  public void onPartitionBecomeDroppedFromError(String partitionName) {
    logger.info("Purging disabled state of dropped replica {} from both InstanceConfig and DataNodeConfig",
        partitionName);
    setPartitionDisabledState(partitionName, false);
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
