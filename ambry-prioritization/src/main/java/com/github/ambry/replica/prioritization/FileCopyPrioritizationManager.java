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

import com.github.ambry.clustermap.AmbryReplica;
import com.github.ambry.clustermap.ClusterManagerQueryHelper;
import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.clustermap.ReplicaState;
import com.github.ambry.replica.prioritization.disruption.DisruptionService;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;


public class FileCopyPrioritizationManager extends Thread implements PrioritizationManager {
  DisruptionService disruptionService;
  String datacenterName;
  ClusterManagerQueryHelper clusterManagerQueryHelper;
  CountDownLatch shutDownLatch = new CountDownLatch(1);
  ReentrantLock lock = new ReentrantLock(true);
  Map<DiskId, LinkedHashSet<ReplicaId>> disIdToReplicaQueue = new HashMap<>();
  Set<ReplicaId> inProgressReplicas = new HashSet<>();
  boolean running = true;

  public FileCopyPrioritizationManager(DisruptionService disruptionService, String datacenterName,
      ClusterManagerQueryHelper clusterManagerQueryHelper) {
    this.disruptionService = disruptionService;
    this.datacenterName = datacenterName;
    this.clusterManagerQueryHelper = clusterManagerQueryHelper;
  }

  @Override
  public void run() {
    while (running) {
      lock.lock();
      disIdToReplicaQueue.keySet().forEach(diskId -> {
        LinkedHashSet<ReplicaId> replicaIds = disIdToReplicaQueue.get(diskId);
        Map<PartitionId, ReplicaId> partitionIdReplicaIdMap = new HashMap<>();
        replicaIds.forEach(replicaId -> {
          partitionIdReplicaIdMap.put(replicaId.getPartitionId(), replicaId);
        });
        List<PartitionId> partitionIds =
            replicaIds.stream().map(ReplicaId::getPartitionId).collect(Collectors.toList());
        List<PartitionId> belowMinActivePartitionIds = filterBelowMinActiveReplicas(partitionIds);
        List<PartitionId> partitionIdsByDisruption = disruptionService.sortByDisruptions(partitionIds);
        LinkedHashSet<PartitionId> partitionIdSet = new LinkedHashSet<>();
        partitionIdSet.addAll(belowMinActivePartitionIds);
        partitionIdSet.addAll(partitionIdsByDisruption);
        LinkedHashSet<ReplicaId> sortedReplicas = new LinkedHashSet<>();
        partitionIdSet.forEach(partitionId -> {
          sortedReplicas.add(partitionIdReplicaIdMap.get(partitionId));
        });
        disIdToReplicaQueue.put(diskId, sortedReplicas);
      });
      lock.unlock();
    }
    shutDownLatch.countDown();
  }

  @Override
  public void shutdown() {
    running = false;
    try {
      shutDownLatch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  @Override
  public List<ReplicaId> getPartitionListForDisk(DiskId diskId, int numberOfReplicasPerDisk) {
    lock.lock();
    LinkedHashSet<ReplicaId> replicaIdSet = disIdToReplicaQueue.getOrDefault(diskId, new LinkedHashSet<>());
    List<ReplicaId> replicaIds = new ArrayList<>(replicaIdSet);

    List<ReplicaId> returnList = replicaIds.subList(0, Integer.min(numberOfReplicasPerDisk, replicaIds.size()));
    List<ReplicaId> remainingList = new ArrayList<>();

    if (returnList.size() < replicaIds.size()) {
      remainingList = replicaIds.subList(returnList.size(), replicaIds.size());
    }
    LinkedHashSet<ReplicaId> remainingSet = new LinkedHashSet<>(remainingList);

    disIdToReplicaQueue.put(diskId, remainingSet);
    inProgressReplicas.addAll(returnList);
    lock.unlock();
    return returnList;
  }

  @Override
  public List<ReplicaId> getInProgressReplicaIdsForDisk(DiskId diskId) {
    lock.lock();
    List<ReplicaId> inProgressReplicasForDisk =
        inProgressReplicas.stream().filter(replicaId -> replicaId.getDiskId() == diskId).collect(Collectors.toList());
    lock.unlock();
    return inProgressReplicasForDisk;
  }

  @Override
  public boolean addReplica(ReplicaId replicaId) {
    lock.lock();
    DiskId diskId = replicaId.getDiskId();
    disIdToReplicaQueue.putIfAbsent(diskId, new LinkedHashSet<>());
    boolean isAlreadyPresent = disIdToReplicaQueue.get(diskId).add(replicaId);
    lock.unlock();
    return isAlreadyPresent;
  }

  @Override
  public boolean removeReplica(DiskId diskId, ReplicaId replicaId) {
    lock.lock();
    boolean wasElementPresent = inProgressReplicas.remove(replicaId);
    lock.unlock();
    return wasElementPresent;
  }

  @Override
  public boolean removeInProgressReplica(DiskId diskId, ReplicaId replicaId) {
    lock.lock();
    boolean wasElementPresent = inProgressReplicas.remove(replicaId);
    lock.unlock();
    return wasElementPresent;
  }

  private List<PartitionId> filterBelowMinActiveReplicas(List<PartitionId> partitionIds) {
    return partitionIds.stream()
        .filter(partitionId -> calculateLocalReplicaCount(partitionId) < getMinActiveReplicas(partitionId))
        .collect(Collectors.toList());
  }

  private int getMinActiveReplicas(PartitionId partitionId) {
    return clusterManagerQueryHelper.getMinActiveReplicas(partitionId);
  }

  private int calculateLocalReplicaCount(PartitionId partition) {
    Set<ReplicaState> states = new HashSet<>(Arrays.asList(ReplicaState.LEADER, ReplicaState.STANDBY));
    Map<ReplicaState, List<AmbryReplica>> localDCReplicas =
        (Map<ReplicaState, List<AmbryReplica>>) partition.getReplicaIdsByStates(states, datacenterName);
    return localDCReplicas.values().stream().mapToInt(List::size).sum();
  }
}
