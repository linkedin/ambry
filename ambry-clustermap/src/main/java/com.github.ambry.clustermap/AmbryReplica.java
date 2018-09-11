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

import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.utils.Utils;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static com.github.ambry.clustermap.ClusterMapUtils.*;


/**
 * {@link ReplicaId} implementation to use within dynamic cluster managers.
 */
class AmbryReplica implements ReplicaId, Resource {
  private final AmbryPartition partition;
  private final AmbryDisk disk;
  private final long capacityBytes;
  private volatile boolean isSealed;
  private volatile boolean isStopped;
  private final ResourceStatePolicy resourceStatePolicy;

  /**
   * Instantiate an AmbryReplica instance.
   * @param clusterMapConfig the {@link ClusterMapConfig} to use.
   * @param partition the {@link AmbryPartition} of which this is a replica.
   * @param disk the {@link AmbryDisk} on which this replica resides.
   * @param isReplicaStopped whether this replica is stopped or not.
   * @param capacityBytes the capacity in bytes for this replica.
   * @param isSealed whether this replica is in sealed state.
   */
  AmbryReplica(ClusterMapConfig clusterMapConfig, AmbryPartition partition, AmbryDisk disk, boolean isReplicaStopped,
      long capacityBytes, boolean isSealed) throws Exception {
    this.partition = partition;
    this.disk = disk;
    this.capacityBytes = capacityBytes;
    this.isSealed = isSealed;
    isStopped = isReplicaStopped;
    validate();
    ResourceStatePolicyFactory resourceStatePolicyFactory =
        Utils.getObj(clusterMapConfig.clusterMapResourceStatePolicyFactory, this, HardwareState.AVAILABLE,
            clusterMapConfig);
    resourceStatePolicy = resourceStatePolicyFactory.getResourceStatePolicy();
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
  public AmbryPartition getPartitionId() {
    return partition;
  }

  @Override
  public AmbryDataNode getDataNodeId() {
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
  public List<AmbryReplica> getPeerReplicaIds() {
    List<AmbryReplica> replicasOfPartition = new ArrayList<>(partition.getReplicaIds());
    replicasOfPartition.remove(this);
    return replicasOfPartition;
  }

  @Override
  public long getCapacityInBytes() {
    return capacityBytes;
  }

  @Override
  public boolean isSealed() {
    return isSealed;
  }

  void setSealedState(boolean isSealed) {
    this.isSealed = isSealed;
  }

  void setStoppedState(boolean isStopped) {
    this.isStopped = isStopped;
  }

  @Override
  public AmbryDisk getDiskId() {
    return disk;
  }

  @Override
  public boolean isDown() {
    return disk.getState() == HardwareState.UNAVAILABLE || resourceStatePolicy.isDown() || isStopped;
  }

  @Override
  public String toString() {
    return "Replica[" + getDataNodeId().getHostname() + ":" + getDataNodeId().getPort() + ":" + getReplicaPath() + "]";
  }

  @Override
  public void markDiskDown() {
    disk.onDiskError();
  }

  @Override
  public void markDiskUp() {
    disk.onDiskOk();
  }

  /**
   * Take actions, if any, when this replica is unavailable.
   */
  void onReplicaUnavailable() {
    resourceStatePolicy.onError();
  }

  /**
   * Take actions, if any, when this replica is back in a good state.
   */
  void onReplicaResponse() {
    resourceStatePolicy.onSuccess();
  }
}

