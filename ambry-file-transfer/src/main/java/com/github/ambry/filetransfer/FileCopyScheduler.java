package com.github.ambry.filetransfer;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.config.FileCopyBasedReplicationConfig;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


public class FileCopyScheduler implements Runnable {
  private final FileCopyBasedReplicationConfig fileCopyBasedReplicationConfig;
  private final FileCopyBasedReplicationMetrics fileCopyBasedReplicationMetrics;
  private final ConcurrentHashMap<DiskId, List<ReplicaId>> diskToReplicaMap;
  private final ConcurrentHashMap<DiskId, FileCopyThread> diskToThreadMap;
  private final List<ReplicaId> inFlightReplicas;
  private final FileCopyHandler fileCopyHandler;
  private final ClusterMap clusterMap;
  private final FileCopyThreadPoolManager fileCopyThreadPoolManager;
  private final Map<ReplicaId, Long> replicaToStartTimeMap;

  public FileCopyScheduler(FileCopyHandler fileCopyHandler,FileCopyBasedReplicationConfig fileCopyBasedReplicationConfig,
      FileCopyBasedReplicationMetrics fileCopyBasedReplicationMetrics, ClusterMap clusterMap,
      FileCopyThreadPoolManager fileCopyThreadPoolManager) {
    this.fileCopyHandler = fileCopyHandler;
    this.fileCopyBasedReplicationConfig = fileCopyBasedReplicationConfig;
    this.fileCopyBasedReplicationMetrics = fileCopyBasedReplicationMetrics;
    this.diskToReplicaMap = new ConcurrentHashMap<>();
    this.diskToThreadMap = new ConcurrentHashMap<>();
    this.inFlightReplicas = new LinkedList<>();
    this.clusterMap = clusterMap;
    this.fileCopyThreadPoolManager = fileCopyThreadPoolManager;
    this.replicaToStartTimeMap = new ConcurrentHashMap<>();
  }

  public void scheduleFileCopy() throws InterruptedException {

    while(true){
      Thread.sleep(1000);

      List<ReplicaId> replicasToDropForHydration = findStarvedReplicas();

      for(ReplicaId replica: replicasToDropForHydration){
        fileCopyThreadPoolManager.removeReplicaForHydration(replica);
        inFlightReplicas.remove(replica);
        replicaToStartTimeMap.remove(replica);
        diskToReplicaMap.get(replica.getDiskId()).remove(replica);
        diskToReplicaMap.get(replica.getDiskId()).add(replica);
      }

      List<DiskId> disksToHydrate = fileCopyThreadPoolManager.provideDisksToHydrate();
      for(DiskId diskId: disksToHydrate){
        ReplicaId replicaId = getNextReplicaToHydrate(diskId);
        if(replicaId != null){
          fileCopyThreadPoolManager.submitReplicaForHydration(replicaId);
          inFlightReplicas.add(replicaId);
          replicaToStartTimeMap.put(replicaId, System.currentTimeMillis()/1000);
        }
      }
    }
  }

  public List<ReplicaId> findStarvedReplicas() {
    //TODO: Manage State To Monitor Time For Replicas In Case Of Restarts
    List<ReplicaId> replicasToDropForHydration = new ArrayList<>();
    for (ReplicaId replicas : inFlightReplicas) {
      if (replicaToStartTimeMap.get(replicas) != null
          && System.currentTimeMillis() / 1000 - replicaToStartTimeMap.get(replicas)
          > fileCopyBasedReplicationConfig.fileCopyReplicaTimeoutSecs) {
        replicasToDropForHydration.add(replicas);
      }
    }
    return replicasToDropForHydration;
  }

  public ReplicaId getNextReplicaToHydrate(DiskId diskId) {
    List<ReplicaId> replicaIds = diskToReplicaMap.get(diskId);
    if(replicaIds == null || replicaIds.isEmpty())
      return null;
    return replicaIds.get(0);
  }

  public void pollForCompletedReplicas(){
    return;
  }


  public boolean createThreadPool(int numberOfThreads){
    fileCopyThreadPoolManager.createThreadPool(numberOfThreads);
  }

  @Override
  public void run() {
    fileCopyThreadPollManager.createThreadPool(fileCopyBasedReplicationConfig.fileCopyNumberOfFileCopyThreads);
    try {
      scheduleFileCopy();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    //TODO: Add Implementation here.
  }

  public synchronized void addReplicaToReplicationPipeline(ReplicaId replicaId) {
    diskToReplicaMap.putIfAbsent(replicaId.getDiskId(), new ArrayList<>());
    diskToReplicaMap.get(replicaId.getDiskId()).add(replicaId);
  }
}
