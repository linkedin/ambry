/*
 * *
 *  * Copyright 2024 LinkedIn Corp. All rights reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 */

package com.github.ambry.replication;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.ReplicaId;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Comparator;
import java.util.Queue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ReplicaBootstrapPrioritizer {
  private static final String STATE_FILE_NAME = "replica_bootstrap_state.json";
  private final String stateFileDirectory;
  private final Map<DiskId, Queue<ReplicaId>> diskToPendingReplicas;
  private final Map<DiskId, ReplicaId> diskToActiveReplica;
  private final ReentrantLock lock;
  private final ObjectMapper objectMapper;
  private static final Logger logger = LoggerFactory.getLogger(ReplicaBootstrapPrioritizer.class);

  public class BootstrapState {
    private Map<String, List<String>> diskToPendingReplicasState;  // DiskId -> List of PartitionIds
    private Map<String, String> diskToActiveReplicaState;          // DiskId -> PartitionId

    // Getters and setters
    public Map<String, List<String>> getDiskToPendingReplicasState() {
      return diskToPendingReplicasState;
    }

    public Map<String, String> getDiskToActiveReplicaState() {
      return diskToActiveReplicaState;
    }

    public void setDiskToPendingReplicasState(Map<String, List<String>> state) {
      this.diskToPendingReplicasState = state;
    }

    public void setDiskToActiveReplicaState(Map<String, String> state) {
      this.diskToActiveReplicaState = state;
    }
  }

  public ReplicaBootstrapPrioritizer(String dataDir) {
    this.stateFileDirectory = dataDir;
    this.diskToPendingReplicas = new HashMap<>();
    this.diskToActiveReplica = new HashMap<>();
    this.lock = new ReentrantLock();
    this.objectMapper = new ObjectMapper();
    loadState();
  }

  private void loadState() {
    lock.lock();
    try {
      File stateFile = new File(stateFileDirectory, STATE_FILE_NAME);
      if (!stateFile.exists()) {
        logger.info("No state file found at {}", stateFile.getAbsolutePath());
        return;
      }

      BootstrapState state = objectMapper.readValue(stateFile, BootstrapState.class);

      // Restore pending replicas
      state.getDiskToPendingReplicasState().forEach((diskPath, partitionIds) -> {
        DiskId diskId = getDiskIdFromPath(diskPath);
        if (diskId != null) {
          Queue<ReplicaId> queue = new PriorityQueue<>(getReplicaComparator());
          partitionIds.forEach(partitionId -> {
            ReplicaId replica = getReplicaFromPartitionId(partitionId, diskId);
            if (replica != null) {
              queue.offer(replica);
            }
          });
          if (!queue.isEmpty()) {
            diskToPendingReplicas.put(diskId, queue);
          }
        }
      });

      // Restore active replicas
      state.getDiskToActiveReplicaState().forEach((diskPath, partitionId) -> {
        DiskId diskId = getDiskIdFromPath(diskPath);
        if (diskId != null) {
          ReplicaId replica = getReplicaFromPartitionId(partitionId, diskId);
          if (replica != null) {
            diskToActiveReplica.put(diskId, replica);
          }
        }
      });
    } catch (IOException e) {
      logger.error("Failed to load bootstrap state", e);
    } finally {
      lock.unlock();
    }
  }

  private void saveState() {
    lock.lock();
    try {
      BootstrapState state = new BootstrapState();

      // Save pending replicas state
      Map<String, List<String>> pendingState = new HashMap<>();
      diskToPendingReplicas.forEach((diskId, queue) -> {
        List<String> partitionIds =
            queue.stream().map(replica -> replica.getPartitionId().toString()).collect(Collectors.toList());
        pendingState.put(diskId.getMountPath(), partitionIds);
      });
      state.setDiskToPendingReplicasState(pendingState);

      // Save active replicas state
      Map<String, String> activeState = new HashMap<>();
      diskToActiveReplica.forEach(
          (diskId, replica) -> activeState.put(diskId.getMountPath(), replica.getPartitionId().toString()));
      state.setDiskToActiveReplicaState(activeState);

      // Write to file
      File stateFile = new File(stateFileDirectory, STATE_FILE_NAME);
      objectMapper.writeValue(stateFile, state);
    } catch (IOException e) {
      logger.error("Failed to save bootstrap state", e);
    } finally {
      lock.unlock();
    }
  }

  public boolean addReplica(ReplicaId replica) {
    lock.lock();
    try {
      DiskId diskId = replica.getDiskId();
      Queue<ReplicaId> diskQueue =
          diskToPendingReplicas.computeIfAbsent(diskId, k -> new PriorityQueue<>(getReplicaComparator()));

      if (!diskToActiveReplica.containsKey(diskId)) {
        diskToActiveReplica.put(diskId, replica);
        saveState();
        return true;
      } else {
        diskQueue.offer(replica);
        saveState();
        return false;
      }
    } finally {
      lock.unlock();
    }
  }

  public ReplicaId onBootstrapComplete(ReplicaId completedReplica) {
    lock.lock();
    try {
      DiskId diskId = completedReplica.getDiskId();
      diskToActiveReplica.remove(diskId);

      Queue<ReplicaId> pendingReplicas = diskToPendingReplicas.get(diskId);
      if (pendingReplicas != null && !pendingReplicas.isEmpty()) {
        ReplicaId nextReplica = pendingReplicas.poll();
        diskToActiveReplica.put(diskId, nextReplica);
        saveState();
        return nextReplica;
      }
      saveState();
      return null;
    } finally {
      lock.unlock();
    }
  }

  private Comparator<ReplicaId> getReplicaComparator() {
    return new ReplicaAvailabilityComparator();
  }

  // Helper methods to convert between IDs and objects
  private DiskId getDiskIdFromPath(String diskPath) {
    // Implementation to get DiskId from path
    // This would need to interact with your cluster manager or configuration
    return null;
  }

  private ReplicaId getReplicaFromPartitionId(String partitionId, DiskId diskId) {
    // Implementation to get ReplicaId from partitionId and diskId
    // This would need to interact with your cluster manager
    return null;
  }
}
