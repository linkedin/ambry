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
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaSyncUpManager;
import com.github.ambry.config.FileCopyBasedReplicationConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.filetransfer.handler.FileCopyHandler;
import com.github.ambry.filetransfer.handler.FileCopyHandlerFactory;
import com.github.ambry.replica.prioritization.PrioritizationManager;
import com.github.ambry.server.StoreManager;
import com.github.ambry.store.FileStoreException;
import com.github.ambry.store.PartitionFileStore;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.store.FileStoreException.FileStoreErrorCode.*;


class FileCopyBasedReplicationSchedulerImpl implements FileCopyBasedReplicationScheduler{
  private final FileCopyBasedReplicationConfig fileCopyBasedReplicationConfig;
  private final FileCopyHandlerFactory fileCopyHandlerFactory;
  private final ClusterMap clusterMap;
  private final FileCopyBasedReplicationThreadPoolManager fileCopyBasedReplicationThreadPoolManager;
  private final Thread fileCopyBasedReplicationThreadPoolManagerThread;
  private final Map<ReplicaId, Long> replicaToStartTimeMap;
  private final Map<ReplicaId, FileCopyStatusListener> replicaToStatusListenerMap;
  private boolean isRunning;
  private final ReplicaSyncUpManager replicaSyncUpManager;
  private final PrioritizationManager prioritizationManager;
  private final List<ReplicaId> inFlightReplicas;
  private final StoreManager storeManager;
  private final StoreConfig storeConfig;
  private final FileCopyMetrics fileCopyMetrics;


  protected final Logger logger = LoggerFactory.getLogger(getClass());

  public FileCopyBasedReplicationSchedulerImpl(@Nonnull FileCopyHandlerFactory fileCopyHandlerFactory,
      FileCopyBasedReplicationConfig fileCopyBasedReplicationConfig,
      ClusterMap clusterMap, @Nonnull PrioritizationManager prioritizationManager,
      @Nonnull ReplicaSyncUpManager replicaSyncUpManager,
      StoreManager storeManager, StoreConfig storeConfig, DataNodeId dataNodeId, FileCopyMetrics fileCopyMetrics){

    Objects.requireNonNull(fileCopyHandlerFactory, "fileCopyHandlerFactory param cannot be null");
    Objects.requireNonNull(fileCopyBasedReplicationConfig, "fileCopyBasedReplicationConfig param cannot be null");
    Objects.requireNonNull(clusterMap, "clusterMap param cannot be null");
    Objects.requireNonNull(prioritizationManager, "prioritizationManager param cannot be null");
    Objects.requireNonNull(replicaSyncUpManager, "replicaSyncUpManager param cannot be null");
    Objects.requireNonNull(storeManager, "storeManager param cannot be null");
    Objects.requireNonNull(storeConfig, "storeConfig param cannot be null");

    this.fileCopyHandlerFactory = fileCopyHandlerFactory;
    this.fileCopyBasedReplicationConfig = fileCopyBasedReplicationConfig;
    this.clusterMap = clusterMap;
    this.fileCopyBasedReplicationThreadPoolManager = new DiskAwareFileCopyThreadPoolManager(dataNodeId.getDiskIds(),
        fileCopyBasedReplicationConfig.fileCopyNumberOfFileCopyThreads, fileCopyMetrics);
    this.fileCopyBasedReplicationThreadPoolManagerThread = new Thread(fileCopyBasedReplicationThreadPoolManager);
    this.replicaToStartTimeMap = new ConcurrentHashMap<>();
    this.inFlightReplicas = new LinkedList<>();
    this.prioritizationManager = prioritizationManager;
    this.replicaSyncUpManager = replicaSyncUpManager;
    this.storeManager = storeManager;
    this.storeConfig = storeConfig;
    this.fileCopyMetrics = fileCopyMetrics;
    this.replicaToStatusListenerMap = new ConcurrentHashMap<>();
  }

  public void run(){
    isRunning = true;
    logger.info("FileCopyBasedReplicationSchedulerImpl Started");
    try {
      fileCopyBasedReplicationThreadPoolManagerThread.start();
      scheduleFileCopy();
    } catch (InterruptedException e) {
      logger.error("Failed to start FileCopy Scheduler", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Find the replicas that are starved for hydration. Return a list of Replicas
   * which are stuck in hydration for more than the configured values.
   * @return the list of {@link ReplicaId} that are starved for hydration.
   */
  List<ReplicaId> findStarvedReplicas() {
    //TODO: Persist Hydration Start Time on Disks For Replicas in Case Of Restarts
    List<ReplicaId> replicasToDropFromHydration = new ArrayList<>();
    for (ReplicaId replica : replicaToStartTimeMap.keySet()) {
      if (replicaToStartTimeMap.get(replica) != null
          && System.currentTimeMillis() / 1000 - replicaToStartTimeMap.get(replica)
          > fileCopyBasedReplicationConfig.fileCopyReplicaTimeoutSecs) {

        logger.info("Replica: {} is starved for hydration. Time since start: {} seconds",
            replica.getPartitionId().toPathString(),
            System.currentTimeMillis() / 1000 - replicaToStartTimeMap.get(replica));
        replicasToDropFromHydration.add(replica);
      }
    }
    return replicasToDropFromHydration;
  }

  List<ReplicaId> getNextReplicaToHydrate(DiskId diskId, int numberOfReplicasOnDisk) {
    List<ReplicaId> replicaIds = prioritizationManager.getPartitionListForDisk(diskId, numberOfReplicasOnDisk);
    if (replicaIds == null) {
      return new ArrayList<>();
    }
    return replicaIds;
  }

  public boolean isRunning() {
    return isRunning;
  }

  @Override
  public void shutdown() throws InterruptedException {
    logger.info("Shutting down FileCopyBasedReplicationSchedulerImpl");
    isRunning = false;
    fileCopyBasedReplicationThreadPoolManager.shutdown();
    fileCopyBasedReplicationThreadPoolManagerThread.join();
    logger.info("FileCopyBasedReplicationSchedulerImpl shutdown");
  }

  @Override
  public void scheduleFileCopy() throws InterruptedException {
    logger.info("Starting File Copy Scheduler");
    while(isRunning){

      Thread.sleep(fileCopyBasedReplicationConfig.fileCopySchedulerWaitTimeSecs*1000);

      List<ReplicaId> replicasToDropForHydration = findStarvedReplicas();
      if(!replicasToDropForHydration.isEmpty()){
        logger.info("Found Replicas To Drop From Hydration: " + replicasToDropForHydration.stream()
            .map(replicaId -> replicaId.getPartitionId().toPathString()).collect(Collectors.toList()));
      } else{
        logger.info("No Replicas To Drop From Hydration In Current Cycle");
      }

      for(ReplicaId replica: replicasToDropForHydration){
        try {
          fileCopyBasedReplicationThreadPoolManager.stopAndRemoveReplicaFromThreadPool(replica);
        } catch (InterruptedException e) {
          //TODO: Send Alert On Failure
          logger.error("Error Stopping Replica: " + replica.getPartitionId().toPathString());
          logger.error("[Error]: ", e);
        }
        replicaToStartTimeMap.remove(replica);
        replicaToStatusListenerMap.get(replica).onFileCopyFailure(new Exception("Replica Timed Out"));
        replicaToStatusListenerMap.remove(replica);
        inFlightReplicas.remove(replica);
        // TODO: The Replica should be sent to the end of the queue inside prioritization Manager
        //  instead of removing it to give other replicas a chance.
        prioritizationManager.removeReplica(replica.getDiskId(), replica);
      }

      List<DiskId> disksToHydrate = fileCopyBasedReplicationThreadPoolManager.getDiskIdsToHydrate();
      for(DiskId diskId: disksToHydrate){
        List<ReplicaId> replicaIds = getNextReplicaToHydrate(diskId, fileCopyBasedReplicationConfig.fileCopyParallelPartitionHydrationCountPerDisk);

        if(!replicaIds.isEmpty()){
          logger.info("Starting Hydration For Disk: {}", diskId);
          for(ReplicaId replicaId: replicaIds) {
            if (inFlightReplicas.contains(replicaId)) {
              continue;
            }
            FileCopyStatusListener fileCopyStatusListener = new FileCopyStatusListenerImpl(replicaSyncUpManager, replicaId);
            FileCopyHandler fileCopyHandler = fileCopyHandlerFactory.getFileCopyHandler();
            try{
              /**
               * Use FileCopyTemporaryDirectoryName to create a temporary directory for file copy.
               * This will be used to write the files which are not yet written and can be cleaned
               * up without
               */
              createTemporaryDirectoryForFileCopyIfAbsent(replicaId, storeConfig);
            } catch (IOException e){
              logger.error("Error Creating Temporary Directory For Replica: " + replicaId.getPartitionId().toPathString());
              fileCopyStatusListener.onFileCopyFailure(e);
              continue;
            }

            fileCopyBasedReplicationThreadPoolManager.submitReplicaForHydration(replicaId,
                fileCopyStatusListener, fileCopyHandler);

            replicaToStatusListenerMap.put(replicaId, fileCopyStatusListener);
            inFlightReplicas.add(replicaId);
            replicaToStartTimeMap.put(replicaId, System.currentTimeMillis()/1000);
          }
        } else{
          logger.info("No Replicas To Hydrate For Disk: " + diskId);
        }
      }
    }

    logger.error("FileCopyBasedReplicationSchedulerImpl Stopped");
  }

  public List<ReplicaId> getInFlightReplicas(){
    return inFlightReplicas;
  }

  public Map<ReplicaId, Long> getReplicaToStartTimeMap(){
    return replicaToStartTimeMap;
  }

  void createTemporaryDirectoryForFileCopyIfAbsent(ReplicaId replica, StoreConfig storeConfig) throws IOException {
    String tempDirPath = replica.getReplicaPath() + File.separator + storeConfig.storeFileCopyTemporaryDirectoryName;
    logger.info("Creating Temporary Directory For File Copy: " + tempDirPath);
    File fileCopyTemporaryDirectory = new File(replica.getReplicaPath(), storeConfig.storeFileCopyTemporaryDirectoryName);
    if (!fileCopyTemporaryDirectory.exists()) {
      fileCopyTemporaryDirectory.mkdirs();
    }
  }

  @Override
  public int getThreadPoolSize() {
    return fileCopyBasedReplicationThreadPoolManager.getThreadPoolSize();
  }

  class FileCopyStatusListenerImpl implements FileCopyStatusListener {

    private final ReplicaSyncUpManager replicaSyncUpManager;
    private final ReplicaId replicaId;

    public FileCopyStatusListenerImpl(ReplicaSyncUpManager replicaSyncUpManager, ReplicaId replicaId) {
      this.replicaSyncUpManager = replicaSyncUpManager;
      this.replicaId = replicaId;
    }

    public ReplicaId getReplicaId() {
      return replicaId;
    }

    @Override
    public void onFileCopySuccess() {
      removeReplicaFromFileCopy(replicaId);
      replicaSyncUpManager.onFileCopyComplete(replicaId);
    }

    @Override
    public void onFileCopyFailure(Exception e) {
      logger.error("Error Copying File For Replica: " + replicaId.getPartitionId().toPathString());
      logger.error("[Error]: ", e);
      try{
        removeReplicaFromFileCopy(replicaId);
      } catch (Exception ex) {
        logger.error("Error Removing Replica From File Copy: " + replicaId.getPartitionId().toPathString(), ex);
      }
      //TODO: update Metrics For File Copy Failure.
      replicaSyncUpManager.onFileCopyError(replicaId);
    }

    void removeReplicaFromFileCopy(ReplicaId replicaId){
      inFlightReplicas.remove(replicaId);
      replicaToStartTimeMap.remove(replicaId);
      prioritizationManager.removeInProgressReplica(replicaId.getDiskId(), replicaId);
      cleanUpStagingDirectory(replicaId);
    }

    void cleanUpStagingDirectory(ReplicaId replicaId){
      try {
          PartitionFileStore fileStore= storeManager.getFileStore(replicaId.getPartitionId());
          fileStore.cleanUpStagingDirectory(replicaId.getReplicaPath(),
              storeConfig.storeFileCopyTemporaryDirectoryName, Long.toString(replicaId.getPartitionId().getId()));
      } catch (IOException e) {
        // if deletion fails, we log here without throwing exception. Next time when server restarts,
        // the store should complete BOOTSTRAP -> STANDBY quickly and attempt to delete this again.
        logger.error("Failed to delete {}", storeConfig.storeFileCopyTemporaryDirectoryName, e);
      } catch (Exception e) {
        // if store is not found, we log here without throwing exception. Next time when server restarts,
        // the store should attempt to delete this again and complete BOOTSTRAP -> STANDBY quickly again.
        logger.error("Failed to get File Store {}", replicaId.getPartitionId().toPathString(), e);
      }
    }
  }
}
