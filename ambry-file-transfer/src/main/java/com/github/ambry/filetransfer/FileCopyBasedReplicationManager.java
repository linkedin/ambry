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
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterParticipant;
import com.github.ambry.clustermap.PartitionStateChangeListener;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaSyncUpManager;
import com.github.ambry.clustermap.StateModelListenerType;
import com.github.ambry.clustermap.StateTransitionException;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.FileCopyBasedReplicationConfig;
import com.github.ambry.network.NetworkClientFactory;
import com.github.ambry.replica.prioritization.PrioritizationManager;
import com.github.ambry.replica.prioritization.PrioritizationManagerFactory;
import com.github.ambry.server.StoreManager;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class FileCopyBasedReplicationManager {

  protected final Logger logger = LoggerFactory.getLogger(getClass());

  private final PrioritizationManager prioritizationManager;
  private final StoreManager storeManager;
  private final FileCopyBasedReplicationConfig fileCopyBasedReplicationConfig;
  private final ClusterParticipant clusterParticipant;
  private final ReplicaSyncUpManager replicaSyncUpManager;
  private final FileCopyBasedReplicationScheduler fileCopyBasedReplicationScheduler;
  private  final NetworkClientFactory networkClientFactory;
  private final ClusterMap clusterMap;
  private boolean isRunning = false;


  public FileCopyBasedReplicationManager(FileCopyBasedReplicationConfig fileCopyBasedReplicationConfig, ClusterMapConfig clusterMapConfig,
     StoreManager storeManager, ClusterMap clusterMap,
      NetworkClientFactory networkClientFactory, MetricRegistry metricRegistry, ClusterParticipant clusterParticipant,
      FileCopyBasedReplicationSchedulerFactory fileCopyBasedReplicationSchedulerFactory,
      PrioritizationManagerFactory prioritizationManagerFactory) throws InterruptedException, InstantiationException {
    this.fileCopyBasedReplicationConfig = fileCopyBasedReplicationConfig;
    this.storeManager = storeManager;

    this.fileCopyBasedReplicationScheduler = fileCopyBasedReplicationSchedulerFactory.getFileCopyBasedReplicationScheduler();
    this.clusterParticipant = clusterParticipant;

    if (clusterParticipant != null) {
      clusterParticipant.registerPartitionStateChangeListener(StateModelListenerType.FileCopyManagerListener,
          new PartitionStateChangeListenerImpl());
      logger.info("File Copy Manager's state change listener registered!");
    }
    this.replicaSyncUpManager = clusterParticipant == null ? null : clusterParticipant.getReplicaSyncUpManager();

    this.prioritizationManager = prioritizationManagerFactory.getPrioritizationManager();
    if(!prioritizationManager.isRunning()) {
      throw new InstantiationException("File Copy cannot run when Prioritization Manager is not running");
    }

    this.networkClientFactory = networkClientFactory;
    this.clusterMap = clusterMap;
  }

  public void start() throws InterruptedException, IOException {
    logger.info("Starting FileCopyBasedReplicationManager");
    fileCopyBasedReplicationScheduler.start();
    isRunning = true;
    logger.info("FileCopyBasedReplicationManager started");
  }

  public void shutdown() throws InterruptedException {
    logger.info("Shutting down FileCopyBasedReplicationManager");
    fileCopyBasedReplicationScheduler.shutdown();
    isRunning = false;
    logger.info("FileCopyBasedReplicationManager shutdown");
  }

  class PartitionStateChangeListenerImpl implements PartitionStateChangeListener {

    @Override
    public void onPartitionBecomeBootstrapFromOffline(String partitionName) throws InterruptedException {
      if(storeManager.getReplica(partitionName) == null){
        if(storeManager.setUpReplica(partitionName)){
          logger.info("Replica setup for partition {} is successful", partitionName);
        } else {
          logger.error("Replica setup for partition {} failed", partitionName);
          throw new StateTransitionException("Replica setup for partition " + partitionName + " failed",
              StateTransitionException.TransitionErrorCode.ReplicaSetUpFailure);
        }
      }
      ReplicaId replicaId = storeManager.getReplica(partitionName);
      replicaSyncUpManager.initiateFileCopy(replicaId);
      prioritizationManager.addReplica(replicaId);
      replicaSyncUpManager.waitForFileCopyCompleted(partitionName);
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