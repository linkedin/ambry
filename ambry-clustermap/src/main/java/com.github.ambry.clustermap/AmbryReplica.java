/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.clustermap;

import java.io.File;
import java.util.List;

import static com.github.ambry.clustermap.ClusterMapUtils.*;


/**
 * {@link ReplicaId} implementation to use within dynamic cluster managers.
 */
class AmbryReplica implements ReplicaId {
  private final AmbryPartition partition;
  private final AmbryDisk disk;
  private final long capacityBytes;

  /**
   * Instantiate an AmbryReplica instance.
   * @param partition the {@link AmbryPartition} of which this is a replica.
   * @param disk the {@link AmbryDisk} on which this replica resides.
   * @param capacityBytes the capacity in bytes for this replica.
   */
  AmbryReplica(AmbryPartition partition, AmbryDisk disk, long capacityBytes) {
    this.partition = partition;
    this.disk = disk;
    this.capacityBytes = capacityBytes;
    validate();
  }

  /**
   * Validate the constructed replica.
   */
  private void validate() {
    if (partition == null || disk == null) {
      throw new IllegalStateException("Null partition " + partition + " or disk: " + disk);
    }
    validateReplicaCapacityInBytes(capacityBytes);
  }

  @Override
  public PartitionId getPartitionId() {
    return partition;
  }

  @Override
  public DataNodeId getDataNodeId() {
    return disk.getDataNode();
  }

  @Override
  public String getMountPath() {
    return disk.getMountPath();
  }

  @Override
  public String getReplicaPath() {
    return disk.getMountPath() + File.separator + partition.toPathString();
  }

  @Override
  public List<? extends ReplicaId> getPeerReplicaIds() {
    List<? extends ReplicaId> replicasOfPartition = partition.getReplicaIds();
    replicasOfPartition.remove(this);
    return replicasOfPartition;
  }

  @Override
  public long getCapacityInBytes() {
    return capacityBytes;
  }

  @Override
  public DiskId getDiskId() {
    return disk;
  }

  @Override
  public boolean isDown() {
    return disk.getState() == HardwareState.UNAVAILABLE;
  }

  @Override
  public String toString() {
    return "Replica[" + getDataNodeId().getHostname() + ":" + getDataNodeId().getPort() + ":" + getReplicaPath() + "]";
  }
}

