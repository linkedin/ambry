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
import java.util.ArrayList;
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

  private final ConcurrentHashMap<DiskId, List<ReplicaId>> inProgressReplicas;

  protected final Logger logger = LoggerFactory.getLogger(getClass());


  public FCFSPrioritizationManager() {
    diskToReplicaMap = new ConcurrentHashMap<>();
    inProgressReplicas = new ConcurrentHashMap<>();
  }
  @Override
  public void start() {
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
      logger.info("No replicas found for disk {}", diskId);
      return null;
    }

    int numberOfReplicasToBeRemoved = Math.min(numberOfReplicasPerDisk, replicaListForDisk.size());
    logger.info("Getting {} replicas for disk {}", numberOfReplicasToBeRemoved, diskId.getMountPath());

    List<ReplicaId> replicasToReturn = new LinkedList<>(replicaListForDisk.subList(0, numberOfReplicasToBeRemoved));

    inProgressReplicas.putIfAbsent(diskId, new ArrayList<>());
    inProgressReplicas.get(diskId).addAll(replicasToReturn);
    replicaListForDisk.subList(0, numberOfReplicasToBeRemoved).clear();

    logger.info("Returning {} replicas for disk {}", numberOfReplicasToBeRemoved, diskId.getMountPath());
    logger.info("Remaining replicas for disk {} are {}", diskId.getMountPath(), replicaListForDisk);

    return replicasToReturn;
  }

  @Override
  public List<ReplicaId> getInProgressReplicaIdsForDisk(DiskId diskId) {
    return inProgressReplicas.get(diskId);
  }

  @Override
  public synchronized boolean addReplica(ReplicaId replicaId) {
    validateIfPzManagerIsRunningOrThrowException(replicaId);
    diskToReplicaMap.putIfAbsent(replicaId.getDiskId(), new LinkedList<>());
    diskToReplicaMap.get(replicaId.getDiskId()).add(replicaId);
    logger.info("Added partition {} to prioritization Manager For Disk {}", replicaId.getReplicaPath(),
        replicaId.getDiskId().getMountPath());
    return true;
  }

  void validateIfPzManagerIsRunningOrThrowException(ReplicaId replicaId){
    if(!isRunning){
      logger.error("Partition {} failed adding to prioritization Manager", replicaId.getReplicaPath());
      throw new StateTransitionException("Partition " + replicaId.getReplicaPath() + " failed adding to "
          + "prioritization Manager", PrioritizationManagerRunningFailure);
    }
  }

  @Override
  public synchronized boolean removeInProgressReplica(DiskId diskId, ReplicaId replicaId){
    return removeReplicaFromMap(diskId, replicaId, true);
  }

  @Override
  public synchronized boolean removeReplica(DiskId diskId, ReplicaId replicaId) {
    return removeReplicaFromMap(diskId, replicaId, false);
  }

  private boolean removeReplicaFromMap(DiskId diskId, ReplicaId replicaId, boolean removeInProgress) {
    validateIfPzManagerIsRunningOrThrowException(replicaId);

    List<ReplicaId> replicaListForDisk;
    if(removeInProgress)
        replicaListForDisk = inProgressReplicas.get(diskId);
    else
        replicaListForDisk = diskToReplicaMap.get(diskId);

    if (replicaListForDisk == null || replicaListForDisk.isEmpty()) {
      return false;
    }
    logger.info("Removed {} partition {} from prioritization Manager For Disk {}", removeInProgress? "InProgress": "", replicaId.getReplicaPath(),
        diskId.getMountPath());
    return replicaListForDisk.remove(replicaId);

  }

  public void reset() {
    diskToReplicaMap.clear();
    inProgressReplicas.clear();
  }
}