/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud;

import com.github.ambry.account.AccountService;
import com.github.ambry.cloud.azure.VcrInstanceConfig;
import com.github.ambry.clustermap.CloudDataNode;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterMapUtils;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.VcrClusterParticipant;
import com.github.ambry.clustermap.VcrClusterParticipantListener;
import com.github.ambry.commons.Callback;
import com.github.ambry.commons.RetryExecutor;
import com.github.ambry.commons.RetryPolicies;
import com.github.ambry.commons.RetryPolicy;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.utils.Utils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.task.Task;
import org.apache.helix.task.TaskCallbackContext;
import org.apache.helix.task.TaskConstants;
import org.apache.helix.task.TaskFactory;
import org.apache.helix.task.TaskStateModelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.clustermap.ClusterMapUtils.*;
import static com.github.ambry.config.CloudConfig.*;


/**
 * Helix Based VCR Cluster.
 */
public class HelixVcrClusterParticipant implements VcrClusterParticipant {
  private static final Logger logger = LoggerFactory.getLogger(HelixVcrClusterParticipant.class);
  private final DataNodeId currentDataNode;
  private final String vcrClusterName;
  private final String vcrInstanceName;
  private final Map<String, PartitionId> partitionIdMap = new ConcurrentHashMap<>();
  private final Set<PartitionId> assignedPartitionIds = ConcurrentHashMap.newKeySet();
  private final HelixVcrClusterMetrics metrics;
  private final List<VcrClusterParticipantListener> listeners = new ArrayList<>();
  private final CloudConfig cloudConfig;
  private final StoreConfig storeConfig;
  private final AccountService accountService;
  private final CloudDestination cloudDestination;
  private HelixManager manager;
  private HelixAdmin helixAdmin;
  private VcrMetrics vcrMetrics;
  private final RetryPolicy retryPolicy = RetryPolicies.exponentialPolicy(3, 2000);
  private final RetryExecutor retryExecutor = new RetryExecutor(Executors.newScheduledThreadPool(2));
  private final ClusterMap clusterMap;

  /**
   * Construct the helix VCR cluster.
   * @param cloudConfig The cloud configuration to use.
   * @param clusterMapConfig The clusterMap configuration to use.
   * @param clusterMap The clusterMap to use.
   * @param vcrMetrics {@link VcrMetrics} object.
   */
  public HelixVcrClusterParticipant(CloudConfig cloudConfig, ClusterMapConfig clusterMapConfig, StoreConfig storeConfig,
      ClusterMap clusterMap, AccountService accountService, CloudDestination cloudDestination, VcrMetrics vcrMetrics) {
    if (Utils.isNullOrEmpty(cloudConfig.vcrClusterZkConnectString)) {
      throw new IllegalArgumentException("Missing value for " + CloudConfig.VCR_CLUSTER_ZK_CONNECT_STRING);
    } else if (Utils.isNullOrEmpty(cloudConfig.vcrClusterName)) {
      throw new IllegalArgumentException("Missing value for " + CloudConfig.VCR_CLUSTER_NAME);
    }
    this.cloudConfig = cloudConfig;
    this.storeConfig = storeConfig;
    currentDataNode = new CloudDataNode(cloudConfig, clusterMapConfig);
    List<? extends PartitionId> allPartitions = clusterMap.getAllPartitionIds(null);
    logger.info("All partitions from clusterMap: {}.", allPartitions);
    vcrClusterName = cloudConfig.vcrClusterName;
    vcrInstanceName =
        ClusterMapUtils.getInstanceName(clusterMapConfig.clusterMapHostName, clusterMapConfig.clusterMapPort);
    metrics = new HelixVcrClusterMetrics(clusterMap.getMetricRegistry(), assignedPartitionIds);
    this.accountService = accountService;
    this.cloudDestination = cloudDestination;
    this.vcrMetrics = vcrMetrics;
    this.clusterMap = clusterMap;
  }

  /**
   * Add {@link PartitionId} to assignedPartitionIds set, if {@param partitionIdStr} valid.
   * Used in one of the vcr state model classes {@link OnlineOfflineHelixVcrStateModel} or
   * {@link LeaderStandbyHelixVcrStateModel} if current VCR is assigned a partition.
   * @param partitionIdStr The partitionIdStr notified by Helix.
   */
  public void addPartition(String partitionIdStr) {
    // Add partition with retry logic in case Ambry clustermap is not update to date(IllegalStateException).
    retryExecutor.runWithRetries(retryPolicy, callback -> doAddPartition(partitionIdStr, callback),
        e -> e instanceof IllegalStateException, (result, exception) -> {
          if (exception != null) {
            // use this condition to capture all potential exceptions.
            logger.warn("AddPartition for {} failed after retry: ", partitionIdStr, exception);
          } else {
            logger.info("Partition {} is added to current VCR: {}. Number of assigned partitions: {}", partitionIdStr,
                vcrInstanceName, assignedPartitionIds.size());
          }
        });
  }

  private void doAddPartition(String partitionIdStr, Callback<Object> callback) {
    PartitionId partitionId = clusterMap.getPartitionIdByName(partitionIdStr);
    if (partitionId == null) {
      metrics.partitionIdNotInClusterMapOnAdd.inc();
      callback.onCompletion(null,
          new IllegalStateException("Partition not in clusterMap on add: Partition Id: " + partitionIdStr));
    } else {
      if (partitionIdMap.putIfAbsent(partitionIdStr, partitionId) == null) {
        // TODO: get rid of assignedPartitionIds
        assignedPartitionIds.add(partitionId);
        for (VcrClusterParticipantListener listener : listeners) {
          listener.onPartitionAdded(partitionId);
        }
        logger.debug("Assigned Partitions: {}", assignedPartitionIds);
      } else {
        logger.info("Partition {} exists on current VCR: {}", partitionIdStr, vcrInstanceName);
      }
      callback.onCompletion(null, null);
    }
  }

  /**
   * Remove {@link PartitionId} from assignedPartitionIds set, if {@param partitionIdStr} valid.
   * Used in one of the vcr state model classes {@link OnlineOfflineHelixVcrStateModel} or
   * {@link LeaderStandbyHelixVcrStateModel} if current VCR becomes offline for a partition.
   * @param partitionIdStr The partitionIdStr notified by Helix.
   */
  public void removePartition(String partitionIdStr) {
    PartitionId partitionId = partitionIdMap.remove(partitionIdStr);
    if (partitionId == null) {
      metrics.partitionIdNotInClusterMapOnRemove.inc();
      logger.warn("Partition {} not exists on current VCR: {}", partitionIdStr, vcrInstanceName);
    } else {
      for (VcrClusterParticipantListener listener : listeners) {
        listener.onPartitionRemoved(partitionId);
      }
      assignedPartitionIds.remove(partitionId);
      logger.info("Partition {} is removed from current VCR: {}. Number of assigned partitions: {}", partitionIdStr,
          vcrInstanceName, assignedPartitionIds.size());
      logger.debug("Current assigned partitions: {}", partitionIdMap.keySet());
    }
  }

  @Override
  public List<? extends DataNodeId> getAllDataNodeIds() {
    // TODO: return all VCR nodes for recovery.
    return Collections.singletonList(currentDataNode);
  }

  @Override
  public DataNodeId getCurrentDataNodeId() {
    return currentDataNode;
  }

  @Override
  public void participate() throws Exception {
    manager = HelixManagerFactory.getZKHelixManager(vcrClusterName, vcrInstanceName, InstanceType.PARTICIPANT,
        cloudConfig.vcrClusterZkConnectString);
    VcrStateModelFactory stateModelFactory = Utils.getObj(cloudConfig.vcrHelixStateModelFactoryClass, this);
    manager.getStateMachineEngine().registerStateModelFactory(stateModelFactory.getStateModelName(), stateModelFactory);
    if (cloudConfig.cloudContainerCompactionEnabled) {
      registerContainerDeletionSyncTask(manager.getStateMachineEngine());
    }
    manager.connect();
    helixAdmin = manager.getClusterManagmentTool();

    InstanceConfig instanceConfig = helixAdmin.getInstanceConfig(vcrClusterName, vcrInstanceName);
    VcrInstanceConfig helixInstanceConfig = VcrInstanceConfig.toVcrInstanceConfig(instanceConfig);
    VcrInstanceConfig newInstanceConfig = new VcrInstanceConfig(cloudConfig.vcrSslPort, cloudConfig.vcrHttp2Port, true);
    if (!helixInstanceConfig.equals(newInstanceConfig))  {
      // Set VCR_HELIX_CONFIG_READY to be true. Listeners take action only when this value is True.
      newInstanceConfig.updateInstanceConfig(instanceConfig);

      logger.info("Update VCR InstanceConfig and set VCR_HELIX_CONFIG_READY to true.");
    } else {
      logger.info("VCR InstanceConfig is the same and VCR_HELIX_CONFIG_READY is true.");
    }
    helixAdmin.setInstanceConfig(vcrClusterName, vcrInstanceName, instanceConfig);
    logger.info("Participated in HelixVcrCluster successfully.");
  }

  /**
   * Register {@link DeprecatedContainerCloudSyncTask}s to sync deleted container information from account service to VCR.
   * @param engine the {@link StateMachineEngine} to register the task state model.
   */
  private void registerContainerDeletionSyncTask(StateMachineEngine engine) {
    if (cloudConfig.cloudContainerCompactionEnabled) {
      Map<String, TaskFactory> taskFactoryMap = new HashMap<>();
      taskFactoryMap.put(DeprecatedContainerCloudSyncTask.COMMAND, new TaskFactory() {
        @Override
        public Task createNewTask(TaskCallbackContext context) {
          return new DeprecatedContainerCloudSyncTask(accountService, storeConfig.storeContainerDeletionRetentionDays,
              cloudDestination, vcrMetrics);
        }
      });
      if (!taskFactoryMap.isEmpty()) {
        if (engine.registerStateModelFactory(TaskConstants.STATE_MODEL_NAME,
            new TaskStateModelFactory(manager, taskFactoryMap))) {
          logger.info("Container deletion sync task registered with helix.");
        } else {
          vcrMetrics.deprecationSyncTaskRegistrationFailureCount.inc();
          logger.error("Container deletion sync task registration with helix failed.");
        }
      }
    }
  }

  @Override
  public Collection<? extends PartitionId> getAssignedPartitionIds() {
    return Collections.unmodifiableCollection(assignedPartitionIds);
  }

  @Override
  public void addListener(VcrClusterParticipantListener listener) {
    listeners.add(listener);
  }

  @Override
  public void close() {
    assignedPartitionIds.clear();
    listeners.clear();
    manager.disconnect();
    helixAdmin.close();
  }
}
