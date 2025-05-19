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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manages the prioritization of file copy operations for replicas across disks.
 * This class ensures that replicas are processed in a prioritized order based on
 * disruption service and replica states, while maintaining thread safety.
 *
 * For each disk it maintains a queue {@link #disIdToReplicaQueue} which gets sorted continuously
 * based on active replicas and data from disruption service. The data can be polled using
 * {@link #getPartitionListForDisk} and partition can be added using {@link #addReplica}.
 */
public class FileCopyPrioritizationManager extends Thread implements PrioritizationManager {
  private final DisruptionService disruptionService;
  private final String datacenterName;
  private final ClusterManagerQueryHelper clusterManagerQueryHelper;
  private final CountDownLatch shutDownLatch = new CountDownLatch(1);
  private final ReentrantLock lock = new ReentrantLock(true);
  private final Map<DiskId, LinkedHashSet<ReplicaId>> disIdToReplicaQueue = new HashMap<>();
  private final Set<ReplicaId> inProgressReplicas = new HashSet<>();
  boolean running = true;

  protected final Logger logger = LoggerFactory.getLogger(getClass());

  public FileCopyPrioritizationManager(DisruptionService disruptionService, String datacenterName,
      ClusterManagerQueryHelper clusterManagerQueryHelper) {
    this.disruptionService = disruptionService;
    this.datacenterName = datacenterName;
    this.clusterManagerQueryHelper = clusterManagerQueryHelper;
  }

  /**
   * This always keeps running
   * For each disk's pending queue, it calculates belowMinActiveReplicas
   * then sorts these by disruption priority and adds to the result, and then it calculates equalMinActiveReplicas
   * then sorts these by disruption priority, and adds these to result next, and it then
   * sorts remaining replicas by disruption priority and adds these result
   * Then the current pending queue is replaced by the result
   */
  @Override
  public void run() {
    while (running) {
      lock.lock();
      try {
        disIdToReplicaQueue.keySet().forEach(diskId -> {
          LinkedHashSet<ReplicaId> replicaIds = disIdToReplicaQueue.get(diskId);
          Map<PartitionId, ReplicaId> partitionIdReplicaIdMap = new HashMap<>();
          replicaIds.forEach(replicaId -> {
            partitionIdReplicaIdMap.put(replicaId.getPartitionId(), replicaId);
          });
          List<PartitionId> partitionIds =
              replicaIds.stream().map(ReplicaId::getPartitionId).collect(Collectors.toList());

          Set<PartitionId> belowMinActivePartitionIds = new HashSet<>(filterBelowMinActiveReplicas(partitionIds));
          Set<PartitionId> equalsMinActivePartitionIds = new HashSet<>(filterEqualsMinActiveReplicas(partitionIds));
          List<PartitionId> partitionIdsByDisruption = disruptionService.sortByDisruptions(partitionIds);

          LinkedHashSet<PartitionId> belowMinActivePartitionIdsByDisruption = new LinkedHashSet<>();
          LinkedHashSet<PartitionId> equalsMinActivePartitionIdsByDisruption = new LinkedHashSet<>();

          partitionIdsByDisruption.forEach(partitionId -> {
            if (belowMinActivePartitionIds.contains(partitionId)) {
              belowMinActivePartitionIdsByDisruption.add(partitionId);
            }
            if (equalsMinActivePartitionIds.contains(partitionId)) {
              equalsMinActivePartitionIdsByDisruption.add(partitionId);
            }
          });

          LinkedHashSet<PartitionId> partitionIdSet = new LinkedHashSet<>();
          partitionIdSet.addAll(belowMinActivePartitionIdsByDisruption);
          partitionIdSet.addAll(equalsMinActivePartitionIdsByDisruption);
          partitionIdSet.addAll(partitionIds);

          LinkedHashSet<ReplicaId> sortedReplicas = new LinkedHashSet<>();
          partitionIdSet.forEach(partitionId -> {
            sortedReplicas.add(partitionIdReplicaIdMap.get(partitionId));
          });
          disIdToReplicaQueue.put(diskId, sortedReplicas);
        });
      } finally {
        lock.unlock();
      }
    }
    shutDownLatch.countDown();
  }

  /**
   * Shuts down the prioritization manager gracefully.
   * Sets {@link #running} to false
   * and waits until run method exits
   */
  @Override
  public void shutdown() {
    logger.info("Shutting down the Prioritization Manager.");
    running = false;
    try {
      shutDownLatch.await();
    } catch (Exception e) {
      logger.error("There was an error while waiting for shut down.", e);
    }
    logger.info("Shut down of the Prioritization Manager completed.");
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  /**
   * Returns the partitions for disk by order of priority
   * and removes these from the queue , everytime different partitions get removed
   * @param diskId the {@link DiskId} for which the list of partitions should be replicated.
   * @param numberOfReplicasPerDisk the number of replicas that should be replicated from the given disk.
   * @return {@link List<ReplicaId>} replica ids for disk
   */
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

  /**
   * Returns the in-progress replicas for the disk
   * @param diskId the {@link DiskId} for which the list of partitions are in progress.
   * @return {@link List<ReplicaId>} replica ids for disk
   */
  @Override
  public List<ReplicaId> getInProgressReplicaIdsForDisk(DiskId diskId) {
    lock.lock();
    List<ReplicaId> inProgressReplicasForDisk =
        inProgressReplicas.stream().filter(replicaId -> replicaId.getDiskId() == diskId).collect(Collectors.toList());
    lock.unlock();
    return inProgressReplicasForDisk;
  }

  /**
   * Adds replicas to a queue which will be prioritized
   * @param replicaId the {@link ReplicaId} to add.
   * @return true if replica is added , false if replica is already present
   */
  @Override
  public boolean addReplica(ReplicaId replicaId) {
    lock.lock();
    DiskId diskId = replicaId.getDiskId();
    disIdToReplicaQueue.putIfAbsent(diskId, new LinkedHashSet<>());
    boolean isAlreadyPresent = disIdToReplicaQueue.get(diskId).add(replicaId);
    lock.unlock();
    return isAlreadyPresent;
  }

  /**
   * Removes the replicas from the queue
   * @param diskId the {@link DiskId} that the replicas are on.
   * @param replicaId the {@link ReplicaId} to remove.
   * @return true if replica was removed, false otherwise
   */
  @Override
  public boolean removeReplica(DiskId diskId, ReplicaId replicaId) {
    lock.lock();
    LinkedHashSet<ReplicaId> replicaQueue = disIdToReplicaQueue.getOrDefault(diskId, new LinkedHashSet<>());
    boolean wasElementPresent = replicaQueue.remove(replicaId);
    lock.unlock();
    return wasElementPresent;
  }

  /**
   * Removes the replicas from in progress and pending queue
   * @param diskId the {@link DiskId} that the replicas are on.
   * @param replicaId the {@link ReplicaId} to remove.
   * @return true if replica was removed, false otherwise
   */
  @Override
  public boolean removeInProgressReplica(DiskId diskId, ReplicaId replicaId) {
    lock.lock();
    boolean wasElementPresent = inProgressReplicas.remove(replicaId);
    LinkedHashSet<ReplicaId> replicaQueue = disIdToReplicaQueue.getOrDefault(diskId, new LinkedHashSet<>());
    replicaQueue.remove(replicaId);
    lock.unlock();
    return wasElementPresent;
  }

  private List<PartitionId> filterBelowMinActiveReplicas(List<PartitionId> partitionIds) {
    return partitionIds.stream()
        .filter(partitionId -> calculateLocalReplicaCount(partitionId) < getMinActiveReplicas(partitionId))
        .collect(Collectors.toList());
  }

  private List<PartitionId> filterEqualsMinActiveReplicas(List<PartitionId> partitionIds) {
    return partitionIds.stream()
        .filter(partitionId -> calculateLocalReplicaCount(partitionId) == getMinActiveReplicas(partitionId))
        .collect(Collectors.toList());
  }

  int getMinActiveReplicas(PartitionId partitionId) {
    return clusterManagerQueryHelper.getMinActiveReplicas(partitionId);
  }

  int calculateLocalReplicaCount(PartitionId partition) {
    Set<ReplicaState> states = new HashSet<>(Arrays.asList(ReplicaState.LEADER, ReplicaState.STANDBY));
    Map<ReplicaState, List<AmbryReplica>> localDCReplicas =
        (Map<ReplicaState, List<AmbryReplica>>) partition.getReplicaIdsByStates(states, datacenterName);
    return localDCReplicas.values().stream().mapToInt(List::size).sum();
  }
}
