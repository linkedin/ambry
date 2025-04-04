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
import com.github.ambry.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class FileCopyBasedReplicationSchedulerImpl implements FileCopyBasedReplicationScheduler{
  private final FileCopyBasedReplicationConfig fileCopyBasedReplicationConfig;
  private final FileCopyHandlerFactory fileCopyHandlerFactory;
  private final ClusterMap clusterMap;
  private final FileCopyBasedReplicationThreadPoolManager fileCopyBasedReplicationThreadPoolManager;
  private final Map<ReplicaId, Long> replicaToStartTimeMap;

  private final Map<ReplicaId, FileCopyStatusListener> replicaToStatusListenerMap;
  private boolean isRunning;
  private final ReplicaSyncUpManager replicaSyncUpManager;
  private final PrioritizationManager prioritizationManager;
  private final List<ReplicaId> inFlightReplicas;
  private final StoreManager storeManager;
  private final StoreConfig storeConfig;


  protected final Logger logger = LoggerFactory.getLogger(getClass());

  public FileCopyBasedReplicationSchedulerImpl(@Nonnull FileCopyHandlerFactory fileCopyHandlerFactory,
      FileCopyBasedReplicationConfig fileCopyBasedReplicationConfig,
      ClusterMap clusterMap, @Nonnull PrioritizationManager prioritizationManager,
      @Nonnull ReplicaSyncUpManager replicaSyncUpManager,
      StoreManager storeManager, StoreConfig storeConfig, DataNodeId dataNodeId){

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
        fileCopyBasedReplicationConfig.fileCopyNumberOfFileCopyThreads);
    this.replicaToStartTimeMap = new ConcurrentHashMap<>();
    this.inFlightReplicas = new LinkedList<>();
    this.prioritizationManager = prioritizationManager;
    this.replicaSyncUpManager = replicaSyncUpManager;
    this.storeManager = storeManager;
    this.storeConfig = storeConfig;
    this.replicaToStatusListenerMap = new ConcurrentHashMap<>();
  }

  @Override
  public void start() throws InterruptedException {
    isRunning = true;
    logger.info("FileCopyBasedReplicationSchedulerImpl Started");
    scheduleFileCopy();
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
        replicasToDropFromHydration.add(replica);
      }
    }
    return replicasToDropFromHydration;
  }

  List<ReplicaId> getNextReplicaToHydrate(DiskId diskId, int numberOfReplicasOnDisk) {
    List<ReplicaId> replicaIds = prioritizationManager.getPartitionListForDisk(diskId, numberOfReplicasOnDisk);
    if(replicaIds == null || replicaIds.isEmpty())
      return null;
    return replicaIds;
  }

  @Override
  public void shutdown() throws InterruptedException {
    isRunning = false;
    fileCopyBasedReplicationThreadPoolManager.shutdown();
  }

  @Override
  public void scheduleFileCopy() throws InterruptedException {

    while(isRunning){

      Thread.sleep(fileCopyBasedReplicationConfig.fileCopySchedulerWaitTimeSecs*1000);

      List<ReplicaId> replicasToDropForHydration = findStarvedReplicas();

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
        logger.info("Starting Hydration For Disk: {} with ReplicaId: {}", diskId, replicaIds.stream().map(replicaId -> replicaId.getPartitionId().toPathString()));

        if(!replicaIds.isEmpty()){
          for(ReplicaId replicaId: replicaIds) {
            if (inFlightReplicas.contains(replicaId)) {
              continue;
            }
            FileCopyStatusListener fileCopyStatusListener = new FileCopyStatusListenerImpl(replicaSyncUpManager, replicaId);
            FileCopyHandler fileCopyHandler = fileCopyHandlerFactory.getFileCopyHandler();
            try{
              /**
               * Adding Persistence of File Copy In Progress File to disk. This will
               * be used for recovery during restarts and rollback/roll forward scenarios.
               */
              createFileCopyInProgressFileIfAbsent(replicaId);
            } catch (IOException e){
              logger.error("Error Creating File Copy In Progress File For Replica: " + replicaId.getPartitionId().toPathString());
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

  /**
   * Create a file copy in progress file on the disk for the replica.
   * @param replica
   * @throws IOException
   */
  void createFileCopyInProgressFileIfAbsent(ReplicaId replica) throws IOException {
    File fileCopyInProgressFileName = new File(replica.getReplicaPath(), storeConfig.storeFileCopyInProgressFileName);
    if (!fileCopyInProgressFileName.exists()) {
      fileCopyInProgressFileName.createNewFile();
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
      removeReplicaFromFileCopy(replicaId);
      //TODO: update Metrics For File Copy Failure.
      replicaSyncUpManager.onFileCopyError(replicaId);
    }

    void removeReplicaFromFileCopy(ReplicaId replicaId){
      inFlightReplicas.remove(replicaId);
      replicaToStartTimeMap.remove(replicaId);
      prioritizationManager.removeReplica(replicaId.getDiskId(), replicaId);
      deleteFileCopyInProgressFile(replicaId);
    }

    void deleteFileCopyInProgressFile(ReplicaId replicaId){
      File fileCopyInProgressFile = new File(replicaId.getReplicaPath(),
          storeConfig.storeFileCopyInProgressFileName);
      try {
        Utils.deleteFileOrDirectory(fileCopyInProgressFile);
      } catch (IOException e) {
        // if deletion fails, we log here without throwing exception. Next time when server restarts,
        // the store should complete BOOTSTRAP -> STANDBY quickly and attempt to delete this again.
        logger.error("Failed to delete {}", fileCopyInProgressFile.getName(), e);
      }
    }
  }
}
