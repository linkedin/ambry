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
package com.github.ambry.cloud;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.server.StoreManager;
import com.github.ambry.store.Store;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The storage manager that does book keeping for all of the partitions handled by this vcr node.
 */
public class CloudStorageManager implements StoreManager {
  private final ReadWriteLock lock;
  private final Map<PartitionId, CloudBlobStore> partitionToStore;
  private final VerifiableProperties properties;
  private final CloudDestination cloudDestination;
  private final VcrMetrics vcrMetrics;
  private final ClusterMap clusterMap;
  private static final Logger logger = LoggerFactory.getLogger(CloudStorageManager.class);

  public CloudStorageManager(VerifiableProperties properties, VcrMetrics vcrMetrics, CloudDestination cloudDestination,
      ClusterMap clusterMap) {
    partitionToStore = new HashMap<>();
    this.properties = properties;
    this.cloudDestination = cloudDestination;
    this.vcrMetrics = vcrMetrics;
    this.clusterMap = clusterMap;
    lock = new ReentrantReadWriteLock();
  }

  @Override
  public boolean addBlobStore(ReplicaId replica) {
    return createAndStartBlobStoreIfAbsent(replica.getPartitionId()) != null;
  }

  @Override
  public boolean shutdownBlobStore(PartitionId id) {
    try {
      lock.writeLock().lock();
      if (partitionToStore.get(id) != null) {
        partitionToStore.get(id).shutdown();
        return true;
      }
    } finally {
      lock.writeLock().unlock();
    }
    return false;
  }

  @Override
  public Store getStore(PartitionId id) {
    try {
      lock.readLock().lock();
      if (partitionToStore.containsKey(id)) {
        return partitionToStore.get(id).isStarted() ? partitionToStore.get(id) : null;
      }
    } finally {
      lock.readLock().unlock();
    }
    return createAndStartBlobStoreIfAbsent(id);
  }

  @Override
  public boolean scheduleNextForCompaction(PartitionId id) {
    throw new UnsupportedOperationException("Method not supported");
  }

  @Override
  public boolean startBlobStore(PartitionId id) {
    try {
      lock.writeLock().lock();
      if (partitionToStore.get(id) != null) {
        partitionToStore.get(id).start();
        return true;
      }
    } finally {
      lock.writeLock().unlock();
    }
    return false;
  }

  @Override
  public boolean controlCompactionForBlobStore(PartitionId id, boolean enabled) {
    throw new UnsupportedOperationException("Method not supported");
  }

  @Override
  public List<PartitionId> setBlobStoreStoppedState(List<PartitionId> partitionIds, boolean markStop) {
    throw new UnsupportedOperationException("Method not supported");
  }

  @Override
  public ReplicaId getReplica(String partitionName) {
    throw new UnsupportedOperationException("Method not supported");
  }

  @Override
  public Collection<PartitionId> getLocalPartitions() {
    throw new UnsupportedOperationException("Method not supported");
  }

  @Override
  public boolean removeBlobStore(PartitionId id) {
    try {
      lock.writeLock().lock();
      return partitionToStore.remove(id) != null;
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public ServerErrorCode checkLocalPartitionStatus(PartitionId partition, ReplicaId localReplica) {
    /*
     * Vcr nodes serve requests for all partitions from cloud blob store. So assuming that cloud is always available,
     * the local partition status for a vcr node should always be available.
     */
    return ServerErrorCode.No_Error;
  }

  /**
   * Return the blobstore for the given partition if blob store if it is already present in {@code CloudStorageManager::partitionToStore}
   * Otherwise create a blobstore for the paritition, and start it.
   * @param partitionId {@code PartitionId} of the store.
   * @return {@code CloudBlobStore} corresponding to the {@code partitionId}
   */
  private CloudBlobStore createAndStartBlobStoreIfAbsent(PartitionId partitionId) {
    try {
      lock.writeLock().lock();
      if (partitionToStore.containsKey(partitionId)) {
        return partitionToStore.get(partitionId);
      }
      CloudBlobStore store = new CloudBlobStore(properties, partitionId, cloudDestination, clusterMap, vcrMetrics);
      partitionToStore.put(partitionId, store);
      store.start();
      return store;
    } finally {
      lock.writeLock().unlock();
    }
  }
}
