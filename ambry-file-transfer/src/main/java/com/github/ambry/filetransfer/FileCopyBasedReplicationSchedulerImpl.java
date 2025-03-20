package com.github.ambry.filetransfer;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaSyncUpManager;
import com.github.ambry.config.FileCopyBasedReplicationConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.replica.prioritization.PrioritizationManager;
import com.github.ambry.server.StoreManager;
import com.github.ambry.store.PartitionFileStore;
import java.nio.file.FileStore;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FileCopyBasedReplicationSchedulerImpl implements FileCopyBasedReplicationScheduler{
  private final FileCopyBasedReplicationConfig fileCopyBasedReplicationConfig;
  private final FileCopyHandler fileCopyHandler;
  private final ClusterMap clusterMap;
  private final FileCopyBasedReplicationThreadPoolManager fileCopyBasedReplicationThreadPoolManager;
  private final Map<ReplicaId, Long> replicaToStartTimeMap;
  private boolean isRunning;
  private final ReplicaSyncUpManager replicaSyncUpManager;

  private final PrioritizationManager prioritizationManager;

  private final StoreManager storeManager;

  private final StoreConfig storeConfig;

  protected final Logger logger = LoggerFactory.getLogger(getClass());

  public FileCopyBasedReplicationSchedulerImpl(FileCopyHandler fileCopyHandler,
      FileCopyBasedReplicationConfig fileCopyBasedReplicationConfig,
      ClusterMap clusterMap, FileCopyBasedReplicationThreadPoolManager fileCopyBasedReplicationThreadPoolManager,
      PrioritizationManager prioritizationManager, ReplicaSyncUpManager replicaSyncUpManager,
      StoreManager storeManager, StoreConfig storeConfig){
    this.fileCopyHandler = fileCopyHandler;
    this.fileCopyBasedReplicationConfig = fileCopyBasedReplicationConfig;
    this.clusterMap = clusterMap;
    this.fileCopyBasedReplicationThreadPoolManager = fileCopyBasedReplicationThreadPoolManager;
    this.replicaToStartTimeMap = new ConcurrentHashMap<>();
    this.prioritizationManager = prioritizationManager;
    this.replicaSyncUpManager = replicaSyncUpManager;
    this.storeManager = storeManager;
    this.storeConfig = storeConfig;
  }
  @Override
  public void start() throws InterruptedException {
    isRunning = true;
    scheduleFileCopy();
  }

  public List<ReplicaId> findStarvedReplicas() {
    //TODO: Persist Hydration Start Time on Disks For Replicas in Case Of Restarts
    List<ReplicaId> replicasToDropForHydration = new ArrayList<>();
    for (ReplicaId replicas : replicaToStartTimeMap.keySet()) {
      if (replicaToStartTimeMap.get(replicas) != null
          && System.currentTimeMillis() / 1000 - replicaToStartTimeMap.get(replicas)
          > fileCopyBasedReplicationConfig.fileCopyReplicaTimeoutSecs) {
        replicasToDropForHydration.add(replicas);
      }
    }
    return replicasToDropForHydration;
  }

  public List<ReplicaId> getNextReplicaToHydrate(DiskId diskId, int numberOfReplicasOnDisk) {
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

      Thread.sleep(1000);
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
      }

      List<DiskId> disksToHydrate = fileCopyBasedReplicationThreadPoolManager.getDiskIdsToHydrate();
      for(DiskId diskId: disksToHydrate){
        List<ReplicaId> replicaIds = getNextReplicaToHydrate(diskId, fileCopyBasedReplicationConfig.fileCopyParallelPartitionHydrationCountPerDisk);
        logger.info("Starting Hydration For Disk: {} with ReplicaId: {}", diskId, replicaIds.stream().map(replicaId -> replicaId.getPartitionId().toPathString()));

        if(!replicaIds.isEmpty()){
          for(ReplicaId replicaId: replicaIds) {
            if (replicaToStartTimeMap.containsKey(replicaId)) {
              continue;
            }
            if(storeManager.isFileExists(replicaId.getPartitionId(), storeConfig.storeFileCopyCompletedFileName){
              logger.info("File Copy Was Completed For Replica: " + replicaId.getPartitionId().toPathString());
              continue;
            }
            //TODO: Add Persistence of File Copy In Progress File to disk when Changing FileCopyHandler
            // to use Asynchronous Network client. Can be used to recover from restarts.
            fileCopyBasedReplicationThreadPoolManager.submitReplicaForHydration(replicaId,
                new FileCopyStatusListenerImpl(replicaSyncUpManager, replicaId), fileCopyHandler);
            replicaToStartTimeMap.put(replicaId, System.currentTimeMillis()/1000);
          }
        } else{
          logger.info("No Replicas To Hydrate For Disk: " + diskId);
        }
      }
    }
  }

  @Override
  public int getThreadPoolSize() {
    return fileCopyBasedReplicationThreadPoolManager.getThreadPoolSize();
  }
}
