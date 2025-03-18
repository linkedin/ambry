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
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionStateChangeListener;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaSyncUpManager;
import com.github.ambry.clustermap.StateModelListenerType;
import com.github.ambry.clustermap.StateTransitionException;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.FileCopyBasedReplicationConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.network.NetworkClientFactory;
import com.github.ambry.replica.prioritization.PrioritizationManager;
import com.github.ambry.server.StoreManager;
import com.github.ambry.store.FileCopySchedulerException;
import com.github.ambry.store.ReplicaSetUpException;
import com.github.ambry.store.StoreKeyFactory;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class FileBasedReplicationManager {

  protected final Logger logger = LoggerFactory.getLogger(getClass());
  protected final StoreManager storeManager;
  private final FileCopyBasedReplicationConfig fileCopyBasedReplicationConfig;
  private final ClusterParticipant clusterParticipant;
  protected final ReplicaSyncUpManager replicaSyncUpManager;
  protected final FileCopyBasedReplicationMetrics fileCopyBasedReplicationMetrics;
  protected final FileCopyScheduler fileCopySchedulerThread;

  public FileBasedReplicationManager(FileCopyHandler fileCopyHandler, PrioritizationManager prioritizationManager, FileCopyBasedReplicationConfig fileCopyBasedReplicationConfig, ClusterMapConfig clusterMapConfig,
      StoreConfig storeConfig, StoreManager storeManager, StoreKeyFactory storeKeyFactory, ClusterMap clusterMap,
      ScheduledExecutorService scheduler, DataNodeId dataNode, NetworkClientFactory networkClientFactory,
      MetricRegistry metricRegistry, ClusterParticipant clusterParticipant, List<? extends ReplicaId> replicaIds) throws InterruptedException {

    this.fileCopyBasedReplicationMetrics = new FileCopyBasedReplicationMetrics(registry, List<? extends ReplicaId > replicaIds);
    this.fileCopyBasedReplicationConfig = fileCopyBasedReplicationConfig;

    this.storeManager = storeManager;

    this.fileCopySchedulerThread = new Thread(new FileCopyScheduler(fileCopyHandler,
        fileCopyBasedReplicationConfig, fileCopyBasedReplicationMetrics, clusterMap));
    try {
      fileCopySchedulerThread.start();
    } catch (FileCopySchedulerException e) {
      //TODO: Update metrics and put Alerts.
      throw new RuntimeException(e);
    }

    if (clusterParticipant != null) {
      clusterParticipant.registerPartitionStateChangeListener(StateModelListenerType.FileCopyManagerListener,
          new PartitionStateChangeListenerImpl());
      logger.info("File Copy Manager's state change listener registered!");
    }

    this.clusterParticipant = clusterParticipant;
    this.replicaSyncUpManager = clusterParticipant == null ? null : clusterParticipant.getReplicaSyncUpManager();
  }

  public void start() throws InterruptedException, IOException {

  }
  class PartitionStateChangeListenerImpl implements PartitionStateChangeListener {

    @Override
    public void onPartitionBecomeBootstrapFromOffline(String partitionName) {
      if(storeManager.getReplica(partitionName) == null) {
        try {
          storeManager.setUpReplica(partitionName);

          fileCopyScheduler.addReplicaToReplicationPipeline();
        } catch (ReplicaSetUpException e) {
          logger.error("Failed to set up replica for partition {} during bootstrap with {}. Error: {}", partitionName,
              StateTransitionException.TransitionErrorCode.FileCopyProtocolFailure, e.getMessage());
          throw new StateTransitionException(e.getMessage(),
              StateTransitionException.TransitionErrorCode.ReplicaSetUpFailure);
        }
      }
      try {
        replicaSyncUpManager.waitForFileCopyCompleted(partitionName);
      } catch (StateTransitionException e) {
        logger.error("Failed to wait for file copy to complete for partition {} during bootstrap with {}. Error: {}",
            partitionName, StateTransitionException.TransitionErrorCode.FileCopyProtocolFailure, e.getMessage());
        throw new StateTransitionException(e.getMessage(), StateTransitionException.TransitionErrorCode.FileCopyProtocolFailure);
      }
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