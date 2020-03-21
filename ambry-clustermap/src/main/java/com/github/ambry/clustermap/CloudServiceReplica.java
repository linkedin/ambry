/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
import org.json.JSONObject;

import static com.github.ambry.clustermap.ClusterMapSnapshotConstants.*;


/**
 * An implementation of {@link AmbryReplica} that is meant to route requests towards cloud destinations using a standard
 * client library. The difference between this and {@link CloudReplica} is that {@link CloudReplica} is meant to
 * represent a VCR host instead of just being used to route requests towards a local client library. Additionally,
 * implementing {@link AmbryReplica} currently allows for easier interop with the rest of the helix cluster management
 * codebase.
 */
class CloudServiceReplica extends AmbryReplica {
  private final CloudServiceDataNode dataNode;

  /**
   * Instantiate a {@link CloudServiceReplica}.
   * @param clusterMapConfig the {@link ClusterMapConfig} to use.
   * @param dataNode the {@link DataNode} to which this replica belongs to. This will usually be localhost, since that
   *                 is where the client library runs from.
   * @param partition the {@link AmbryPartition} of which this is a replica.
   * @param capacityBytes the capacity in bytes for this replica.
   */
  CloudServiceReplica(ClusterMapConfig clusterMapConfig, CloudServiceDataNode dataNode, AmbryPartition partition,
      long capacityBytes) throws Exception {
    super(clusterMapConfig, partition, false, capacityBytes, false);
    this.dataNode = dataNode;
  }

  @Override
  public AmbryDisk getDiskId() {
    throw new UnsupportedOperationException("No disk for CloudServiceReplica");
  }

  @Override
  public AmbryDataNode getDataNodeId() {
    return dataNode;
  }

  @Override
  public String getMountPath() {
    throw new UnsupportedOperationException("No mount path for CloudServiceReplica");
  }

  @Override
  public String getReplicaPath() {
    throw new UnsupportedOperationException("No replica path for CloudServiceReplica");
  }

  @Override
  public ReplicaType getReplicaType() {
    return ReplicaType.CLOUD_BACKED;
  }

  @Override
  public JSONObject getSnapshot() {
    JSONObject snapshot = new JSONObject();
    snapshot.put(REPLICA_PARTITION, getPartitionId().toPathString());
    snapshot.put(REPLICA_TYPE, getReplicaType());
    snapshot.put(CAPACITY_BYTES, getCapacityInBytes());
    snapshot.put(REPLICA_WRITE_STATE, isSealed() ? PartitionState.READ_ONLY.name() : PartitionState.READ_WRITE.name());
    String replicaLiveness = UP;
    if (isStopped) {
      replicaLiveness = REPLICA_STOPPED;
    } else if (resourceStatePolicy.isHardDown()) {
      replicaLiveness = DOWN;
    } else if (resourceStatePolicy.isDown()) {
      replicaLiveness = SOFT_DOWN;
    }
    snapshot.put(LIVENESS, replicaLiveness);
    return snapshot;
  }

  @Override
  public String toString() {
    return "Replica[cloud:" + getPartitionId().toPathString() + "]";
  }

  @Override
  public void markDiskDown() {
    throw new UnsupportedOperationException("No disk for CloudServiceReplica");
  }

  @Override
  public void markDiskUp() {
    throw new UnsupportedOperationException("No disk for CloudServiceReplica");
  }
}
