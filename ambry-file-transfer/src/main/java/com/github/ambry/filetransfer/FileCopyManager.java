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
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.PartitionStateChangeListener;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.StateModelListenerType;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.FileCopyBasedReplicationConfig;
import com.github.ambry.config.FileCopyConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.network.NetworkClientFactory;
import com.github.ambry.replica.prioritization.PrioritizationManager;
import com.github.ambry.server.StoreManager;
import com.github.ambry.store.FileStore;
import com.github.ambry.store.StoreException;
import com.github.ambry.store.StoreKeyFactory;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class FileCopyManager {

  protected final Logger logger = LoggerFactory.getLogger(getClass());
  protected final PrioritizationManager prioritizationManager;

  private final StoreManager storeManager;

  private final FileCopyConfig fileCopyConfig;

  private final StoreConfig storeConfig;

  public FileCopyManager(PrioritizationManager prioritizationManager, FileCopyBasedReplicationConfig fileCopyBasedReplicationConfig, ClusterMapConfig clusterMapConfig,
      FileCopyConfig fileCopyConfig, StoreConfig storeConfig, StoreManager storeManager, StoreKeyFactory storeKeyFactory, ClusterMap clusterMap,
      ScheduledExecutorService scheduler, DataNodeId dataNode, NetworkClientFactory networkClientFactory,
      MetricRegistry metricRegistry, ClusterParticipant clusterParticipant) throws InterruptedException {
    this.fileCopyConfig = fileCopyConfig;
    this.storeConfig = storeConfig;
    if (clusterParticipant != null) {
      clusterParticipant.registerPartitionStateChangeListener(StateModelListenerType.FileCopyManagerListener,
          new PartitionStateChangeListenerImpl());
      logger.info("File Copy Manager's state change listener registered!");
    }
    this.prioritizationManager = prioritizationManager;
    if(!prioritizationManager.isRunning()) {
      prioritizationManager.start();
    }

    this.storeManager = storeManager;
  }
  public void start() throws InterruptedException, IOException {

  }

  public void callMetaDataAPI(){

  }
  class PartitionStateChangeListenerImpl implements PartitionStateChangeListener {

    @Override
    public void onPartitionBecomeBootstrapFromOffline(String partitionName) {
      if(storeManager.getReplica(partitionName) == null){
        storeManager.setUpReplica(partitionName);
      }
      ReplicaId replicaId = storeManager.getReplica(partitionName);
      if(storeManager.getFileStore(replicaId.getPartitionId()) == null){
        storeManager.addFileStore(replicaId);
        try {
          storeManager.getFileStore(replicaId.getPartitionId()).start();
        } catch (StoreException e) {
          throw new RuntimeException(e);
        }
      }
      if(!storeManager.isFileExists(replicaId.getPartitionId(), storeConfig.storeFileCopyCompletedFileName)){
        FileStore fileStore = storeManager.getFileStore(replicaId.getPartitionId());
        PartitionId partitionId = replicaId.getPartitionId();
        List<ReplicaId> replicaIds = (List<ReplicaId>) partitionId.getReplicaIds();
        //TODO : Find the
        String hostName = replicaIds.get(0).getDataNodeId().getHostname();

        //callMetaData
        //store metadata
        //chunk API caller
        //build state
      }else{
        storeManager.buildStateForFileCopy(replicaId);
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