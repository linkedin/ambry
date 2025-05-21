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
 * For each disk it maintains a queue {@link #diskIdToReplicaQueue} which gets sorted continuously
 * based on active replicas and data from disruption service. The data can be polled using
 * {@link #getPartitionListForDisk} and partition can be added using {@link #addReplica}.
 */
public class FileCopyPrioritizationManager extends Thread implements PrioritizationManager {
  private final DisruptionService disruptionService;
  private final String datacenterName;
  private final ClusterManagerQueryHelper clusterManagerQueryHelper;
  private final CountDownLatch shutDownLatch = new CountDownLatch(1);
  private final ReentrantLock lock = new ReentrantLock(true);
  private final Map<DiskId, LinkedHashSet<ReplicaId>> diskIdToReplicaQueue = new HashMap<>();
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
      runPrioritizationCycle();
    }
    shutDownLatch.countDown();
  }

  public void runPrioritizationCycle() {
    try {
      //TODO - Take lock on disk level for improved add/remove performance and add notify based timer to shutdown
      lock.lock();
      diskIdToReplicaQueue.keySet().forEach(diskId -> {
        LinkedHashSet<ReplicaId> replicaIds = diskIdToReplicaQueue.get(diskId);
        if (replicaIds.isEmpty()) {
          return;
        }
        logger.trace("current queue at starting cycle disk {} is {}", diskId, replicaIds);
        Map<PartitionId, ReplicaId> partitionIdReplicaIdMap = new HashMap<>();
        replicaIds.forEach(replicaId -> {
          partitionIdReplicaIdMap.put(replicaId.getPartitionId(), replicaId);
        });
        List<PartitionId> partitionIds =
            replicaIds.stream().map(ReplicaId::getPartitionId).collect(Collectors.toList());
        logger.trace("at starting partition ids are for disk {} are {}", diskId, partitionIds);

        Set<PartitionId> belowMinActivePartitionIds =
            new HashSet<>(filterBelowMinActiveReplicas(new ArrayList<>(partitionIds)));
        Set<PartitionId> equalsMinActivePartitionIds =
            new HashSet<>(filterEqualsMinActiveReplicas(new ArrayList<>(partitionIds)));
        List<PartitionId> partitionIdsByDisruption = disruptionService.sortByDisruptions(new ArrayList<>(partitionIds));

        logger.trace("below min active partition ids disk {} partition {}", diskId, belowMinActivePartitionIds);
        logger.trace("below equals active partition ids disk {} partition {}", diskId, equalsMinActivePartitionIds);
        logger.trace("partition ids by disruption for disk {} partition {}", diskId, partitionIdsByDisruption);

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
        partitionIdSet.addAll(partitionIdsByDisruption);
        partitionIdSet.addAll(partitionIds);

        logger.trace("all partition ids at end are for disk {} partition {}", diskId, partitionIds);
        logger.trace("below min partition sorted are for disk {} partition {}", diskId,
            belowMinActivePartitionIdsByDisruption);
        logger.trace("equals min partition sorted are for disk {} partiton {}", diskId, equalsMinActivePartitionIds);
        logger.trace("all partition ids sorted are for disk {} partition {}", diskId, partitionIdSet);

        LinkedHashSet<ReplicaId> sortedReplicas = new LinkedHashSet<>();
        partitionIdSet.forEach(partitionId -> {
          sortedReplicas.add(partitionIdReplicaIdMap.get(partitionId));
        });
        logger.trace("current queue for disk {} is {}", diskId, sortedReplicas);

        LinkedHashSet<ReplicaId> prevReplicas = diskIdToReplicaQueue.get(diskId);
        logIfQueueChanged(diskId, prevReplicas, sortedReplicas);
        diskIdToReplicaQueue.put(diskId, sortedReplicas);
      });
    } finally {
      lock.unlock();
    }
  }

  private void logIfQueueChanged(DiskId diskId, LinkedHashSet<ReplicaId> previousQueue,
      LinkedHashSet<ReplicaId> newQueue) {
    if (previousQueue.size() != newQueue.size()) {
      logger.info("For disk {} queue has changed from {} to {}", diskId, previousQueue, newQueue);
      return;
    }

    List<ReplicaId> previousQueueList = new ArrayList<>(previousQueue);
    List<ReplicaId> newQueueList = new ArrayList<>(newQueue);

    for (int i = 0; i < previousQueueList.size(); i++) {
      if (previousQueueList.get(i).equals(newQueueList.get(i))) {
        logger.info("For disk {} queue has changed from {} to {}", diskId, previousQueue, newQueue);
      }
    }
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
    LinkedHashSet<ReplicaId> replicaIdSet = diskIdToReplicaQueue.getOrDefault(diskId, new LinkedHashSet<>());
    List<ReplicaId> replicaIds = new ArrayList<>(replicaIdSet);

    List<ReplicaId> returnList = replicaIds.subList(0, Integer.min(numberOfReplicasPerDisk, replicaIds.size()));
    List<ReplicaId> remainingList = new ArrayList<>();

    if (returnList.size() < replicaIds.size()) {
      remainingList = replicaIds.subList(returnList.size(), replicaIds.size());
    }
    LinkedHashSet<ReplicaId> remainingSet = new LinkedHashSet<>(remainingList);

    diskIdToReplicaQueue.put(diskId, remainingSet);
    inProgressReplicas.addAll(returnList);
    lock.unlock();
    logger.info("Returning replicas for disk {} are {}", diskId, returnList);
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
    logger.info("Adding replica {}", replicaId);
    DiskId diskId = replicaId.getDiskId();
    diskIdToReplicaQueue.putIfAbsent(diskId, new LinkedHashSet<>());
    boolean isAlreadyPresent = diskIdToReplicaQueue.get(diskId).add(replicaId);
    logger.trace("after adding queue for disk {} is {}", diskId, diskIdToReplicaQueue.get(diskId));
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
    logger.info("removing replica {} from disk {}", replicaId, diskId);
    LinkedHashSet<ReplicaId> replicaQueue = diskIdToReplicaQueue.getOrDefault(diskId, new LinkedHashSet<>());
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
    logger.info("Removing in progress replica {} disk {}", replicaId, diskId);
    boolean wasElementPresent = inProgressReplicas.remove(replicaId);
    LinkedHashSet<ReplicaId> replicaQueue = diskIdToReplicaQueue.getOrDefault(diskId, new LinkedHashSet<>());
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
