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
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The storage manager that does book keeping for all of the partitions handled by this vcr node.
 */
public class CloudStorageManager implements StoreManager {
  private final ConcurrentMap<PartitionId, CloudBlobStore> partitionToStore;
  private final VerifiableProperties properties;
  private final CloudDestination cloudDestination;
  private final VcrMetrics vcrMetrics;
  private final ClusterMap clusterMap;
  private static final Logger logger = LoggerFactory.getLogger(CloudStorageManager.class);

  public CloudStorageManager(VerifiableProperties properties, VcrMetrics vcrMetrics, CloudDestination cloudDestination,
      ClusterMap clusterMap) {
    partitionToStore = new ConcurrentHashMap<>();
    this.properties = properties;
    this.cloudDestination = cloudDestination;
    this.vcrMetrics = vcrMetrics;
    this.clusterMap = clusterMap;
  }

  @Override
  public boolean addBlobStore(ReplicaId replica) {
    createAndStartBlobStoreIfAbsent(replica.getPartitionId());
    return partitionToStore.get(replica.getPartitionId()) != null;
  }

  @Override
  public boolean shutdownBlobStore(PartitionId id) {
    CloudBlobStore blobStore = partitionToStore.get(id);
    if (blobStore == null) {
      return false;
    }
    blobStore.shutdown();
    return true;
  }

  @Override
  public Store getStore(PartitionId id) {
    createAndStartBlobStoreIfAbsent(id);
    return partitionToStore.get(id);
  }

  @Override
  public boolean scheduleNextForCompaction(PartitionId id) {
    throw new UnsupportedOperationException("Method not supported");
  }

  @Override
  public boolean startBlobStore(PartitionId id) {
    CloudBlobStore cloudStore = partitionToStore.get(id);
    if (cloudStore == null) {
      return false;
    }
    cloudStore.start();
    return true;
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
  public boolean removeBlobStore(PartitionId id) {
    return partitionToStore.remove(id) != null;
  }

  @Override
  public ServerErrorCode checkLocalPartitionStatus(PartitionId partition, ReplicaId localReplica) {
    /*
     * Vcr nodes serve requests for all partitions from cloud blob store. So assuming that cloud is always available,
     * the local partition status for a vcr node should always be available.
     */
    return ServerErrorCode.No_Error;
  }

  private void createAndStartBlobStoreIfAbsent(PartitionId partitionId) {
    partitionToStore.computeIfAbsent(partitionId,
        value -> new CloudBlobStore(properties, partitionId, cloudDestination, clusterMap, vcrMetrics));
    // cloud blob store start is idempotent
    partitionToStore.get(partitionId).start();
    logger.info("Cloudblobstore started for partition {}", partitionId);
  }
}
