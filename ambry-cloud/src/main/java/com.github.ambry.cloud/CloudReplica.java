/*
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

import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.DiskId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.ReplicaId;
import com.github.ambry.config.CloudConfig;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONObject;

import static com.github.ambry.clustermap.ClusterMapSnapshotConstants.*;


/**
 * {@link ReplicaId} implementation to use within virtual cloud replicator.
 */
class CloudReplica implements ReplicaId {
  private final PartitionId partitionId;
  private final CloudDataNode cloudDataNode;
  private final String mountPathPrefix;

  /**
   * Instantiate an CloudReplica instance.
   * @param cloudConfig the {@link CloudConfig} to use.
   * @param partitionId the {@link PartitionId} of which this is a replica.
   * @param cloudDataNode the {@link CloudDataNode} of which this replica on.
   *
   */
  CloudReplica(CloudConfig cloudConfig, PartitionId partitionId, CloudDataNode cloudDataNode) {
    this.partitionId = partitionId;
    this.cloudDataNode = cloudDataNode;
    this.mountPathPrefix = cloudConfig.vcrReplicaMountPathPrefix;
  }

  @Override
  public PartitionId getPartitionId() {
    return partitionId;
  }

  @Override
  public DataNodeId getDataNodeId() {
    return cloudDataNode;
  }

  @Override
  public String getMountPath() {
    return mountPathPrefix + partitionId.toPathString();
  }

  @Override
  public String getReplicaPath() {
    return getMountPath() + File.separator + partitionId.toPathString();
  }

  @Override
  public List<ReplicaId> getPeerReplicaIds() {
    return new ArrayList<>(partitionId.getReplicaIds());
  }

  @Override
  public long getCapacityInBytes() {
    return -1;
  }

  @Override
  public boolean isSealed() {
    return false;
  }

  @Override
  public JSONObject getSnapshot() {
    JSONObject snapshot = new JSONObject();
    DataNodeId dataNodeId = getDataNodeId();
    snapshot.put(REPLICA_NODE, dataNodeId.getHostname() + ":" + dataNodeId.getPort());
    snapshot.put(REPLICA_PARTITION, getPartitionId().toPathString());
    snapshot.put(REPLICA_PATH, getReplicaPath());
    snapshot.put(CAPACITY_BYTES, getCapacityInBytes());
    return snapshot;
  }

  @Override
  public DiskId getDiskId() {
    return null;
  }

  @Override
  public boolean isDown() {
    return false;
  }

  @Override
  public String toString() {
    return "CloudReplica[" + getDataNodeId().getHostname() + ":" + getDataNodeId().getPort() + ":" + getReplicaPath()
        + "]";
  }

  @Override
  public void markDiskDown() {
  }

  @Override
  public void markDiskUp() {
  }
}

