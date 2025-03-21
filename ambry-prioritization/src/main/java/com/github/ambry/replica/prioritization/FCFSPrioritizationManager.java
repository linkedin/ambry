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

package com.github.ambry.replica.prioritization;

import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.ReplicaId;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;


/**
 * The FCFSPrioritizationManager is a First-Come-First-Serve implementation of the PrioritizationManager.
 */
public class FCFSPrioritizationManager implements PrioritizationManager {
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
  public List<ReplicaId> getPartitionListForDisk(@Nonnull DiskId diskId, @Nonnegative int numberOfReplicasPerDisk) {
    List<ReplicaId> replicaListForDisk = diskToReplicaMap.get(diskId);

    if(replicaListForDisk == null)
      return null;
    LinkedList<ReplicaId> listsToReturn = new LinkedList<>();

    int numberOfReplicasToBeRemoved = Math.min(numberOfReplicasPerDisk, replicaListForDisk.size());
    for(int index = 0; index < numberOfReplicasToBeRemoved; index++){
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