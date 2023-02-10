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
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.github.ambry.clustermap.ClusterMapUtils.*;


/**
 * {@link ReplicaId} implementation to use within dynamic cluster managers.
 */
public abstract class AmbryReplica implements ReplicaId {
  private final AmbryPartition partition;
  private final long capacityBytes;
  private volatile ReplicaSealStatus replicaSealStatus;
  volatile boolean isStopped;
  final ResourceStatePolicy resourceStatePolicy;

  /**
   * Instantiate an AmbryReplica instance.
   * @param clusterMapConfig the {@link ClusterMapConfig} to use.
   * @param partition the {@link AmbryPartition} of which this is a replica.
   * @param isReplicaStopped whether this replica is stopped or not.
   * @param capacityBytes the capacity in bytes for this replica.
   * @param replicaSealStatus {@link ReplicaSealStatus} of this replica.
   */
  AmbryReplica(ClusterMapConfig clusterMapConfig, AmbryPartition partition, boolean isReplicaStopped,
      long capacityBytes, ReplicaSealStatus replicaSealStatus) throws Exception {
    this.partition = Objects.requireNonNull(partition, "null partition");
    this.capacityBytes = capacityBytes;
    this.replicaSealStatus = replicaSealStatus;
    isStopped = isReplicaStopped;
    validateReplicaCapacityInBytes(capacityBytes);
    ResourceStatePolicyFactory resourceStatePolicyFactory =
        Utils.getObj(clusterMapConfig.clusterMapResourceStatePolicyFactory, this, HardwareState.AVAILABLE,
            clusterMapConfig);
    resourceStatePolicy = resourceStatePolicyFactory.getResourceStatePolicy();
  }

  @Override
  public abstract AmbryDataNode getDataNodeId();

  @Override
  public abstract AmbryDisk getDiskId();

  @Override
  public AmbryPartition getPartitionId() {
    return partition;
  }

  @Override
  public List<AmbryReplica> getPeerReplicaIds() {
    return partition.getReplicaIds().stream().filter(replica -> !this.equals(replica)).collect(Collectors.toList());
  }

  @Override
  public long getCapacityInBytes() {
    return capacityBytes;
  }

  @Override
  public boolean isSealed() {
    return replicaSealStatus == ReplicaSealStatus.SEALED;
  }

  @Override
  public boolean isPartiallySealed() {
    return replicaSealStatus == ReplicaSealStatus.PARTIALLY_SEALED;
  }

  /**
   * Set the {@link ReplicaSealStatus} of this replica.
   * @param replicaSealStatus {@link ReplicaSealStatus} to set.
   */
  void setSealedStatus(ReplicaSealStatus replicaSealStatus) {
    this.replicaSealStatus = replicaSealStatus;
  }

  /**
   * @return ReplicaSealStatus of this replica.
   */
  ReplicaSealStatus getSealedStatus() {
    return this.replicaSealStatus;
  }

  /**
   * Set the stopped state of this replica.
   * @param isStopped {@code true} to mark replica as stopped. {@code false} otherwise.
   */
  void setStoppedState(boolean isStopped) {
    this.isStopped = isStopped;
  }

  /**
   * @return {@code true} is the replica is down. {@code false} otherwise.
   */
  @Override
  public boolean isDown() {
    return resourceStatePolicy.isDown() || isStopped;
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

