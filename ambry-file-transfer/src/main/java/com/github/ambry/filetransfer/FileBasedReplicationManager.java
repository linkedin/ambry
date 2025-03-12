/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.filetransfer;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.clustermap.AmbryReplicaSyncUpManager;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterParticipant;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionStateChangeListener;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.StateModelListenerType;
import com.github.ambry.clustermap.StateTransitionException;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.FileCopyBasedReplicationConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.network.NetworkClientFactory;
import com.github.ambry.replica.prioritization.PrioritizationManager;
import com.github.ambry.server.StoreManager;
import com.github.ambry.store.StoreKeyFactory;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.clustermap.StateTransitionException.TransitionErrorCode.*;


public class FileBasedReplicationManager {

  protected final Logger logger = LoggerFactory.getLogger(getClass());
  protected final PrioritizationManager prioritizationManager;

  private final StoreManager storeManager;

  private final AmbryReplicaSyncUpManager ambryReplicaSyncUpManager;

  private final StoreConfig storeConfig;

  private final ClusterMap clusterMap;

  private final DataNodeId dataNodeId;

  private final NetworkClientFactory networkClientFactory;

  private final MetricRegistry metricRegistry;

  private final FileCopyBasedReplicationSchedulerFactory fileCopyBasedReplicationSchedulerFactory;

  private final FileCopyBasedReplicationThreadPoolManager fileCopyBasedReplicationThreadPoolManager;

  private final ScheduledExecutorService scheduledExecutorService;

  private final FileCopyBasedReplicationScheduler fileCopyBasedReplicationScheduler;

  public FileBasedReplicationManager(FileCopyHandler fileCopyHandler, PrioritizationManager prioritizationManager, FileCopyBasedReplicationConfig fileCopyBasedReplicationConfig, ClusterMapConfig clusterMapConfig,
      StoreConfig storeConfig, StoreManager storeManager, ClusterMap clusterMap,
      ScheduledExecutorService scheduledExecutorService, DataNodeId dataNodeId, NetworkClientFactory networkClientFactory,
      MetricRegistry metricRegistry, ClusterParticipant clusterParticipant,
      FileCopyBasedReplicationSchedulerFactory fileCopyBasedReplicationSchedulerFactory,
      FileCopyBasedReplicationThreadPoolManager fileCopyBasedReplicationThreadPoolManager, AmbryReplicaSyncUpManager ambryReplicaSyncUpManager)
      throws InterruptedException, InstantiationException {

    this.storeConfig = storeConfig;
    this.clusterMap = clusterMap;
    this.dataNodeId = dataNodeId;
    this.networkClientFactory = networkClientFactory;
    this.metricRegistry = metricRegistry;

    this.fileCopyBasedReplicationSchedulerFactory = fileCopyBasedReplicationSchedulerFactory;
    this.fileCopyBasedReplicationScheduler = fileCopyBasedReplicationSchedulerFactory.getFileCopyBasedReplicationScheduler();

    this.fileCopyBasedReplicationThreadPoolManager = fileCopyBasedReplicationThreadPoolManager;

    if (clusterParticipant != null) {
      clusterParticipant.registerPartitionStateChangeListener(StateModelListenerType.FileCopyManagerListener,
          new PartitionStateChangeListenerImpl());
      logger.info("File Copy Manager's state change listener registered!");
    }
    if(!prioritizationManager.isRunning()){
      logger.error("Failed to Instantiate Prioritization Manager, File Copy Manager cannot be started");
      throw new InstantiationException("Failed to Start File Copy Exception, Prioritization Manager is not running");
    }

    this.prioritizationManager = prioritizationManager;
    this.storeManager = storeManager;
    this.ambryReplicaSyncUpManager = ambryReplicaSyncUpManager;
    this.scheduledExecutorService = scheduledExecutorService;
  }

  class PartitionStateChangeListenerImpl implements PartitionStateChangeListener {

    @Override
    public void onPartitionBecomeBootstrapFromOffline(String partitionName) {
      if(storeManager.getReplica(partitionName) == null){
        //storeManager.setUpReplica(partitionName);
      }
<<<<<<< Updated upstream
      prioritizationManager.addReplica(partitionName);
=======
      ReplicaId replicaId = storeManager.getReplica(partitionName);
      if(prioritizationManager.addReplica(replicaId)){
        logger.info("Replica {} added to prioritization manager", replicaId);
      } else {
        logger.error("Replica {} could not be added to prioritization manager", replicaId);
        throw new StateTransitionException("Replica: " + partitionName + " could not be added to prioritization manager", PrioritizationManagerFailure);
      }
      try{
        ambryReplicaSyncUpManager.waitForFileCopyCompleted(partitionName);
      } catch (InterruptedException e) {
        logger.error("Interrupted while waiting for file copy to complete for partition {} with exception: {}" , partitionName, FileCopyProtocolFailure);
        throw new StateTransitionException("Interrupted while waiting for file copy to complete for partition: "
            + partitionName, FileCopyProtocolFailure);
      } catch (Exception e) {
        logger.error("Exception while waiting for file copy to complete for partition {} with exception: {}", partitionName, e.toString());
        throw new StateTransitionException("Exception while waiting for file copy to complete for partition: "
            + partitionName, FileCopyProtocolFailure);
      }
>>>>>>> Stashed changes
    }

    @Override
    public void onPartitionBecomeStandbyFromBootstrap(String partitionName) {

    }

    @Override
    public void onPartitionBecomeLeaderFromStandby(String partitionName) {

    }

    @Override
    public void onPartitionBecomeStandbyFromLeader(String partitionName) {

    }

    @Override
    public void onPartitionBecomeInactiveFromStandby(String partitionName) {

    }

    @Override
    public void onPartitionBecomeOfflineFromInactive(String partitionName) {

    }

    @Override
    public void onPartitionBecomeDroppedFromOffline(String partitionName) {

    }
  }
}