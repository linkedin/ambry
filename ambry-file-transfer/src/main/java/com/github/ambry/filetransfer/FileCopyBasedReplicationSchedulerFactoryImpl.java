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
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.ReplicaSyncUpManager;
import com.github.ambry.config.FileCopyBasedReplicationConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.filetransfer.handler.FileCopyHandlerFactory;
import com.github.ambry.replica.prioritization.PrioritizationManager;
import com.github.ambry.server.StoreManager;
import java.util.Objects;


public class FileCopyBasedReplicationSchedulerFactoryImpl implements FileCopyBasedReplicationSchedulerFactory {

  private final FileCopyHandlerFactory fileCopyHandlerFactory;
  private final FileCopyBasedReplicationConfig fileCopyBasedReplicationConfig;
  private final ClusterMap clusterMap;
  private final PrioritizationManager prioritizationManager;
  private final StoreManager storeManager;
  private final StoreConfig storeConfig;
  private final DataNodeId dataNodeId;
  private final ReplicaSyncUpManager replicaSyncUpManager;

  public FileCopyBasedReplicationSchedulerFactoryImpl(FileCopyHandlerFactory fileCopyHandlerFactory,
      FileCopyBasedReplicationConfig fileCopyBasedReplicationConfig, ClusterMap clusterMap,
      PrioritizationManager prioritizationManager, StoreManager storeManager, StoreConfig storeConfig, DataNodeId dataNodeId, ClusterParticipant clusterParticipant) {

    Objects.requireNonNull(clusterParticipant, "ClusterParticipant cannot be null");

    this.fileCopyHandlerFactory = fileCopyHandlerFactory;
    this.fileCopyBasedReplicationConfig = fileCopyBasedReplicationConfig;
    this.clusterMap = clusterMap;
    this.prioritizationManager = prioritizationManager;
    this.storeManager = storeManager;
    this.storeConfig = storeConfig;
    this.dataNodeId = dataNodeId;
    this.replicaSyncUpManager = clusterParticipant.getReplicaSyncUpManager();
  }

  @Override
  public FileCopyBasedReplicationScheduler getFileCopyBasedReplicationScheduler() {
    return new FileCopyBasedReplicationSchedulerImpl(fileCopyHandlerFactory, fileCopyBasedReplicationConfig, clusterMap,
        prioritizationManager, replicaSyncUpManager, storeManager, storeConfig, dataNodeId);
  }
}