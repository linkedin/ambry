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