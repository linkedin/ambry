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

package com.github.ambry.replica.prioritization;

import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.StateTransitionException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.clustermap.StateTransitionException.TransitionErrorCode.*;


/**
 * The FCFSPrioritizationManager is a First-Come-First-Serve implementation of the PrioritizationManager.
 */
public class FCFSPrioritizationManager implements PrioritizationManager {
  private boolean isRunning;
  private final ConcurrentHashMap<DiskId, List<ReplicaId>> diskToReplicaMap;

  protected final Logger logger = LoggerFactory.getLogger(getClass());

  public FCFSPrioritizationManager() {
    System.out.println("FCFS Inititalized");
    diskToReplicaMap = new ConcurrentHashMap<>();
  }
  @Override
  public void start() {
    logger.info("FCH TEST: FCH TEST: FCFS Pz Manager has Started");
    isRunning = true;
  }

  @Override
  public void shutdown() {
    reset();
    isRunning = false;
  }

  @Override
  public boolean isRunning() {
    return isRunning;
  }

  @Override
  public List<ReplicaId> getPartitionListForDisk(@Nonnull DiskId diskId, @Nonnegative int numberOfReplicasPerDisk) {
    if(!isRunning){
      logger.error("Failed to get partition list for disk {}", diskId);
      throw new StateTransitionException("Failed to get partition list for disk " + diskId, PrioritizationManagerRunningFailure);
    }

    List<ReplicaId> replicaListForDisk = diskToReplicaMap.get(diskId);

    if(replicaListForDisk == null){
      logger.error("No replicas found for disk {}", diskId);
      return null;
    }

    int numberOfReplicasToBeRemoved = Math.min(numberOfReplicasPerDisk, replicaListForDisk.size());
    logger.info("FCH TEST: Getting {} replicas for disk {}", numberOfReplicasToBeRemoved, diskId.getMountPath());


    // Retrieve and remove the replicas
    List<ReplicaId> replicasToReturn = new LinkedList<>(replicaListForDisk.subList(0, numberOfReplicasToBeRemoved));
    replicaListForDisk.subList(0, numberOfReplicasToBeRemoved).clear();
    logger.info("FCH TEST: Returning {} replicas for disk {}", numberOfReplicasToBeRemoved, diskId.getMountPath());
    logger.info("FCH TEST: Remaining replicas for disk {} are {}", diskId.getMountPath(), replicaListForDisk.size());
    logger.info("FCH TEST: Remaining replicas for disk {} are {}", diskId.getMountPath(), replicaListForDisk);
    return replicasToReturn;
  }

  @Override
  public synchronized boolean addReplica(ReplicaId replicaId) {
    logger.info("FCH TEST: Trying to Add Replica to FCFS");
    validateIfPzManagerIsRunningOrThrowException(replicaId);
    logger.info("FCH TEST: Prioritizer is Running");
    diskToReplicaMap.putIfAbsent(replicaId.getDiskId(), new LinkedList<>());
    diskToReplicaMap.get(replicaId.getDiskId()).add(replicaId);
    logger.info("FCH TEST: Added partition {} to prioritization Manager For Disk {}", replicaId.getReplicaPath(),
        replicaId.getDiskId().getMountPath());
    return true;
  }

  boolean validateIfPzManagerIsRunningOrThrowException(ReplicaId replicaId){
    if(!isRunning){
      logger.error("Partition {} failed adding to prioritization Manager", replicaId.getReplicaPath());
      throw new StateTransitionException("Partition " + replicaId.getReplicaPath() + " failed adding to "
          + "prioritization Manager", PrioritizationManagerRunningFailure);
    }
    else
      return true;
  }

  @Override
  public synchronized boolean removeReplica(DiskId diskId, ReplicaId replicaId) {
    if(!isRunning){
      logger.error("Partition {} failed removing from prioritization Manager", replicaId.getReplicaPath());
      throw new StateTransitionException("Partition " + replicaId.getReplicaPath() + " failed removing from "
          + "prioritization Manager", PrioritizationManagerRunningFailure);
    }

    List<ReplicaId> replicaListForDisk = diskToReplicaMap.get(diskId);
    if(replicaListForDisk == null || replicaListForDisk.isEmpty()){
      return false;
    }
    logger.info("FCH TEST: Removed partition {} from prioritization Manager For Disk {}", replicaId.getReplicaPath(),
        diskId.getMountPath());
    return replicaListForDisk.remove(replicaId);
  }

  @Override
  public int getNumberOfDisks() {
    return diskToReplicaMap.size();
  }

  @Override
  public void reset() {
    diskToReplicaMap.clear();
  }
}