/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.replication;

import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.Store;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;


public class PartitionInfo {

  private final List<RemoteReplicaInfo> remoteReplicas;
  private final PartitionId partitionId;
  private final Store store;
  private final ReplicaId localReplicaId;
  private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
  // Only in VCR, we set the replicaThread because we want a single thread in VCR to take care of a partition and
  // all its replicas. This is unlike server-server replication, where multiple threads are working on multiple replicas
  // of a partition. Check selectReplicas().
  private ReplicaThread replicaThread = null;

  public PartitionInfo(List<RemoteReplicaInfo> remoteReplicas, PartitionId partitionId, Store store,
      ReplicaId localReplicaId) {
    this.remoteReplicas = remoteReplicas;
    this.partitionId = partitionId;
    this.store = store;
    this.localReplicaId = localReplicaId;
  }

  // Only used in VCR, not server-server replication
  public void setReplicaThread(ReplicaThread replicaThread) {
    this.replicaThread = replicaThread;
  }

  // Only used in VCR, not server-server replication
  public ReplicaThread getReplicaThread() {
    return replicaThread;
  }

  public PartitionId getPartitionId() {
    return partitionId;
  }

  public List<RemoteReplicaInfo> getRemoteReplicaInfos() {
    rwLock.readLock().lock();
    try {
      return remoteReplicas;
    } finally {
      rwLock.readLock().unlock();
    }
  }

  public Store getStore() {
    return store;
  }

  public ReplicaId getLocalReplicaId() {
    return this.localReplicaId;
  }

  /**
   * Add {@link RemoteReplicaInfo} to this {@link PartitionInfo} if it is previously absent.
   * @param remoteReplicaInfo the {@link RemoteReplicaInfo} to add.
   * @return {@code true} if remote replica info is added. {@code false} if it is already present
   */
  boolean addReplicaInfoIfAbsent(RemoteReplicaInfo remoteReplicaInfo) {
    rwLock.writeLock().lock();
    boolean isAdded = false;
    try {
      List<RemoteReplicaInfo> foundSameReplica = remoteReplicas.stream()
          .filter(info -> info.getReplicaId() == remoteReplicaInfo.getReplicaId())
          .collect(Collectors.toList());
      if (foundSameReplica.isEmpty()) {
        remoteReplicas.add(remoteReplicaInfo);
        isAdded = true;
      }
    } finally {
      rwLock.writeLock().unlock();
    }
    return isAdded;
  }

  /**
   * Remove {@link RemoteReplicaInfo} of given replica from this {@link PartitionInfo} if it is present.
   * @param remoteReplica the {@link ReplicaId} whose info should be removed.
   * @return {@link RemoteReplicaInfo} that is removed, can be null if it is not present.
   */
  RemoteReplicaInfo removeReplicaInfoIfPresent(ReplicaId remoteReplica) {
    rwLock.writeLock().lock();
    RemoteReplicaInfo replicaInfoToRemove = null;
    try {
      for (RemoteReplicaInfo remoteReplicaInfo : remoteReplicas) {
        if (remoteReplicaInfo.getReplicaId().getDataNodeId() == remoteReplica.getDataNodeId()) {
          replicaInfoToRemove = remoteReplicaInfo;
          break;
        }
      }
      if (replicaInfoToRemove != null) {
        remoteReplicas.remove(replicaInfoToRemove);
      }
    } finally {
      rwLock.writeLock().unlock();
    }
    return replicaInfoToRemove;
  }

  /**
   * Go through remote replicas of this partition and compare messages newly written to store with messages
   * found missing during the previous replication cycle. This is used during leader-based cross colo replication where
   * missing store messages found in metadata exchange are not fetched for non-leader replica pairs (i.e. either local replica or
   * remote replica is not a leader of their partition) and are expected to come from leader pair exchanges via intra-dc replication.
   * @param messagesWrittenToStore list of messages written to local store.
   */
  void updateReplicaInfosOnMessageWrite(List<MessageInfo> messagesWrittenToStore) {
    rwLock.readLock().lock();
    try {
      for (RemoteReplicaInfo remoteReplicaInfo : remoteReplicas) {
        remoteReplicaInfo.updateMissingMessagesInMetadataResponse(messagesWrittenToStore);
      }
    } finally {
      rwLock.readLock().unlock();
    }
  }

  @Override
  public String toString() {
    return partitionId.toString() + " " + remoteReplicas.toString();
  }
}
