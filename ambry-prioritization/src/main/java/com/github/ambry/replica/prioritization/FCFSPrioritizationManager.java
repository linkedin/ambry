package com.github.ambry.replica.prioritization;

import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.ReplicaId;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;


public class FCFSPrioritizationManager implements PrioritizationManager{
  private boolean isRunning;

  private final ConcurrentHashMap<DiskId, List<ReplicaId>> diskToReplicaMap;
  public FCFSPrioritizationManager() {
    isRunning = false;
    diskToReplicaMap = new ConcurrentHashMap<>();
  }
  @Override
  public void start() {
    isRunning = true;
  }

  @Override
  public void shutdown() {
    isRunning = false;
  }

  @Override
  public boolean isRunning() {
    return isRunning;
  }

  @Override
  public List<ReplicaId> getPartitionListForDisk(DiskId diskId, int numberOfReplicasPerDisk) {
    List<ReplicaId> replicaListForDisk = diskToReplicaMap.get(diskId);

    if(replicaListForDisk == null)
      return null;
    LinkedList<ReplicaId> listsToReturn = new LinkedList<>();

    for( int index = 0; index < Math.min(numberOfReplicasPerDisk, replicaListForDisk.size());index++){
      listsToReturn.add(replicaListForDisk.get(index));
      replicaListForDisk.remove(index);
    }

    return listsToReturn;
  }

  @Override
  public synchronized boolean addReplica(ReplicaId replicaId) {
    if(!isRunning){
      return false;
    }
    diskToReplicaMap.putIfAbsent(replicaId.getDiskId(), new LinkedList<>());
    diskToReplicaMap.get(replicaId.getDiskId()).add(replicaId);
    return true;
  }

  @Override
  public synchronized boolean removeReplica(DiskId diskId, ReplicaId replicaId) {
    if(!isRunning){
      return false;
    }
    List<ReplicaId> replicaListForDisk = diskToReplicaMap.get(diskId);
    if(replicaListForDisk == null){
      return false;
    }
    return replicaListForDisk.remove(replicaId);
  }

  @Override
  public int getNumberOfDisks(int numberOfDisks) {
    return diskToReplicaMap.size();
  }
}
