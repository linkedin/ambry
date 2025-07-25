/**
 * Copyright 2025 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.ClusterParticipant;
import com.github.ambry.clustermap.PartitionStateChangeListener;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaSyncUpManager;
import com.github.ambry.clustermap.StateModelListenerType;
import com.github.ambry.clustermap.StateTransitionException;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.FileCopyBasedReplicationConfig;
import com.github.ambry.config.ReplicaPrioritizationConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.filetransfer.handler.FileCopyHandlerFactory;
import com.github.ambry.network.NetworkClientFactory;
import com.github.ambry.replica.prioritization.PrioritizationManager;
import com.github.ambry.server.StoreManager;
import com.github.ambry.store.Store;
import java.io.IOException;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FileCopyBasedReplicationManager {

  protected final Logger logger = LoggerFactory.getLogger(getClass());

  private final PrioritizationManager prioritizationManager;
  private final StoreManager storeManager;
  private final FileCopyBasedReplicationConfig fileCopyBasedReplicationConfig;
  private final ClusterParticipant clusterParticipant;
  private final ReplicaSyncUpManager replicaSyncUpManager;
  private final FileCopyMetrics fileCopyMetrics;
  private final FileCopyBasedReplicationScheduler fileCopyBasedReplicationScheduler;
  private final Thread fileCopyBasedReplicationSchedulerThread;
  private final NetworkClientFactory networkClientFactory;
  private final ClusterMap clusterMap;
  private final StoreConfig storeConfig;
  private boolean isRunning = false;
  private final FileCopyHandlerFactory fileCopyHandlerFactory;

  public FileCopyBasedReplicationManager(FileCopyBasedReplicationConfig fileCopyBasedReplicationConfig,
      ClusterMapConfig clusterMapConfig, StoreManager storeManager, ClusterMap clusterMap,
      NetworkClientFactory networkClientFactory, FileCopyMetrics fileCopyMetrics, ClusterParticipant clusterParticipant,
      FileCopyBasedReplicationSchedulerFactory fileCopyBasedReplicationSchedulerFactory,
      FileCopyHandlerFactory fileCopyHandlerFactory, PrioritizationManager prioritizationManager,
      StoreConfig storeConfig, ReplicaPrioritizationConfig replicaPrioritizationConfig) throws InstantiationException {

    Objects.requireNonNull(fileCopyBasedReplicationConfig, "FileCopyBasedReplicationConfig cannot be null");
    Objects.requireNonNull(clusterMapConfig, "ClusterMapConfig cannot be null");
    Objects.requireNonNull(storeManager, "StoreManager cannot be null");
    Objects.requireNonNull(clusterMap, "ClusterMap cannot be null");
    Objects.requireNonNull(networkClientFactory, "NetworkClientFactory cannot be null");
    Objects.requireNonNull(fileCopyMetrics, "FileCopyMetrics cannot be null");
    Objects.requireNonNull(fileCopyBasedReplicationSchedulerFactory,
        "FileCopyBasedReplicationSchedulerFactory cannot be null");
    Objects.requireNonNull(prioritizationManager, "PrioritizationManager cannot be null");
    Objects.requireNonNull(storeConfig, "StoreConfig cannot be null");
    Objects.requireNonNull(fileCopyHandlerFactory, "FileCopyHandlerFactory cannot be null");
    Objects.requireNonNull(replicaPrioritizationConfig, "ReplicaPrioritizationConfig cannot be null");

    this.fileCopyBasedReplicationConfig = fileCopyBasedReplicationConfig;
    this.storeManager = storeManager;

    this.clusterParticipant = clusterParticipant;
    this.fileCopyHandlerFactory = fileCopyHandlerFactory;

    if (clusterParticipant != null) {
      clusterParticipant.registerPartitionStateChangeListener(StateModelListenerType.FileCopyManagerListener,
          new PartitionStateChangeListenerImpl());
      logger.info("File Copy Manager's state change listener registered!");
    } else {
      throw new InstantiationException("File Copy Manager cannot be instantiated without a ClusterParticipant");
    }
    this.replicaSyncUpManager = clusterParticipant == null ? null : clusterParticipant.getReplicaSyncUpManager();

    this.prioritizationManager = prioritizationManager;
    this.fileCopyMetrics = fileCopyMetrics;
    this.fileCopyBasedReplicationScheduler =
        fileCopyBasedReplicationSchedulerFactory.getFileCopyBasedReplicationScheduler();
    this.fileCopyBasedReplicationSchedulerThread = new Thread(fileCopyBasedReplicationScheduler);
    if (!prioritizationManager.isRunning()) {
      throw new InstantiationException("File Copy cannot run when Prioritization Manager is not running");
    }

    this.networkClientFactory = networkClientFactory;
    this.clusterMap = clusterMap;
    this.storeConfig = storeConfig;
  }

  public void start() throws InterruptedException, IOException {
    logger.info("Starting FileCopyBasedReplicationManager");
    fileCopyBasedReplicationSchedulerThread.start();
    isRunning = true;
    logger.info("FileCopyBasedReplicationManager started");
  }

  public void shutdown() throws InterruptedException {
    logger.info("Shutting down FileCopyBasedReplicationManager");
    fileCopyBasedReplicationScheduler.shutdown();
    fileCopyBasedReplicationSchedulerThread.join();
    isRunning = false;
    logger.info("FileCopyBasedReplicationManager shutdown");
  }

  class PartitionStateChangeListenerImpl implements PartitionStateChangeListener {

    @Override
    public void onPartitionBecomeBootstrapFromOffline(String partitionName) {
      boolean initiatingFileCopy = false;
      try {
        fileCopyMetrics.incrementPartitionInFileCopyPath();
        /**
         * If the store is already started or is not initialized, then we should not do file copy again.
         * We should skip file copy and just return.
         * This scenario will occur when the server is restarted and the partition was already registered with the node.
         * The restarted automatically triggers the staging directory clean up and removes any residual incomplete copied files from the previous File copy run.
         */
        Store store = storeManager.getInitializedStore(storeManager.getReplica(partitionName).getPartitionId());

        if (store == null) {
          logger.error("Store for Partition {} is null. Ignoring state change", partitionName);
          return;
        }

        if (store.isStarted()) {
          logger.info("Store for Partition {} is already started. Ignoring state change", partitionName);
          return;
        }

        if (!isRunning) {
          logger.error("FileCopyBasedReplicationManager is not running. Ignoring state change for partition: {}",
              partitionName);
          throw new StateTransitionException(
              "FileCopyBasedReplicationManager is not running. Ignoring state " + "change for partition: "
                  + partitionName,
              StateTransitionException.TransitionErrorCode.FileCopyBasedReplicationManagerNotRunning);
        }

        ReplicaId replicaId = storeManager.getReplica(partitionName);

        if (replicaId == null) {
          // Replica set up should have succeeded before this state transition.
          logger.error("Replica setup for partition {} failed", partitionName);
          throw new StateTransitionException("Replica setup for partition " + partitionName + " failed",
              StateTransitionException.TransitionErrorCode.ReplicaSetUpFailure);
        }

        /**
         * If the file copy was already completed, then no need to do it again.
         */
        if (storeManager.isFileExists(replicaId.getPartitionId(), storeConfig.storeFileCopyCompletedFileName)) {
          logger.info("File Copy Was Completed For Replica: " + replicaId.getPartitionId().toPathString());
          return;
        }

        logger.info("Initiated File Copy Wait On ReplicaSyncUpManager for Replica: {}",
            replicaId.getPartitionId().toPathString());
        initiatingFileCopy = true;
        replicaSyncUpManager.initiateFileCopy(replicaId);

        logger.info("Adding Replica to Prioritization Manager For Replica: {}",
            replicaId.getPartitionId().toPathString());
        prioritizationManager.addReplica(replicaId);
      } finally {
        fileCopyMetrics.decrementPartitionInFileCopyPath();
        if (initiatingFileCopy) {
          fileCopyMetrics.incrementFileCopyInitiated();
        } else {
          fileCopyMetrics.incrementFileCopySkipped();
        }
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