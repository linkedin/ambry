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
import com.github.ambry.store.Store;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;


public class PartitionInfo {

  private final List<RemoteReplicaInfo> remoteReplicas;
  private final PartitionId partitionId;
  private final Store store;
  private final ReplicaId localReplicaId;
  private final ReentrantLock lock = new ReentrantLock();

  public PartitionInfo(List<RemoteReplicaInfo> remoteReplicas, PartitionId partitionId, Store store,
      ReplicaId localReplicaId) {
    this.remoteReplicas = remoteReplicas;
    this.partitionId = partitionId;
    this.store = store;
    this.localReplicaId = localReplicaId;
  }

  public PartitionId getPartitionId() {
    return partitionId;
  }

  public List<RemoteReplicaInfo> getRemoteReplicaInfos() {
    return remoteReplicas;
  }

  public Store getStore() {
    return store;
  }

  public ReplicaId getLocalReplicaId() {
    return this.localReplicaId;
  }

  boolean addReplicaInfoIfAbsent(RemoteReplicaInfo remoteReplicaInfo) {
    lock.lock();
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
      lock.unlock();
    }
    return isAdded;
  }

  boolean removeRelicaInfoIfPresent(RemoteReplicaInfo remoteReplicaInfo) {
    lock.lock();
    boolean isRemoved;
    try {
      isRemoved = remoteReplicas.remove(remoteReplicaInfo);
    } finally {
      lock.unlock();
    }
    return isRemoved;
  }

  @Override
  public String toString() {
    return partitionId.toString() + " " + remoteReplicas.toString();
  }
}
