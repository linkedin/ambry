/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.clustermap.ClusterMapSnapshotConstants.*;


/**
 * An implementation of {@link ReplicaId} to be used within the {@link StaticClusterManager}.
 *
 * A Replica is one constituent piece of a {@link Partition}. A Replica is uniquely identifiable by its Partition and
 * its {@link Disk}. Note that this induces a constraint that a Partition can never have more than one Replica on a
 * given Disk. This ensures that a Partition does not have Replicas that share fates.
 */
class Replica implements ReplicaId {
  private final Partition partition;
  private Disk disk;
  private volatile boolean isStopped = false;
  private final ResourceStatePolicy resourceStatePolicy;
  private final ReplicaType replicaType;
  private final ClusterMapConfig clusterMapConfig;

  private Logger logger = LoggerFactory.getLogger(getClass());

  Replica(Partition partition, Disk disk, ClusterMapConfig clusterMapConfig) {
    if (logger.isTraceEnabled()) {
      logger.trace("Replica " + partition + ", " + disk);
    }
    this.partition = partition;
    this.disk = disk;
    this.clusterMapConfig = clusterMapConfig;
    try {
      ResourceStatePolicyFactory resourceStatePolicyFactory =
          Utils.getObj(clusterMapConfig.clusterMapResourceStatePolicyFactory, this, HardwareState.AVAILABLE,
              clusterMapConfig);
      resourceStatePolicy = resourceStatePolicyFactory.getResourceStatePolicy();
    } catch (Exception e) {
      logger.error("Error creating resource state policy when instantiating a replica " + e);
      throw new IllegalStateException("Error creating resource state policy when instantiating a replica " + partition,
          e);
    }
    if (disk.getMountPath().startsWith(CLOUD_REPLICA_MOUNT)) {
      replicaType = ReplicaType.CLOUD_BACKED;
    } else {
      replicaType = ReplicaType.DISK_BACKED;
    }
    validate();
  }

  Replica(HardwareLayout hardwareLayout, Partition partition, JSONObject jsonObject) throws JSONException {
    this(partition, hardwareLayout.findDisk(jsonObject.getString("hostname"), jsonObject.getInt("port"),
        jsonObject.getString("mountPath")), hardwareLayout.getClusterMapConfig());
  }

  @Override
  public PartitionId getPartitionId() {
    return getPartition();
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
    return getMountPath() + File.separator + partition.toPathString();
  }

  @Override
  public List<ReplicaId> getPeerReplicaIds() {
    List<ReplicaId> peerReplicas = getPeerReplicas();
    return new ArrayList<>(peerReplicas);
  }

  @Override
  public long getCapacityInBytes() {
    return partition.getReplicaCapacityInBytes();
  }

  @Override
  public DiskId getDiskId() {
    return disk;
  }

  @Override
  public boolean isDown() {
    return getDataNodeId().getState() == HardwareState.UNAVAILABLE
        || getDiskId().getState() == HardwareState.UNAVAILABLE || resourceStatePolicy.isDown() || isStopped;
  }

  @Override
  public boolean isSealed() {
    return partition.getPartitionState().equals(PartitionState.READ_ONLY);
  }

  @Override
  public JSONObject getSnapshot() {
    JSONObject snapshot = new JSONObject();
    DataNodeId dataNodeId = getDataNodeId();
    snapshot.put(REPLICA_NODE, dataNodeId.getHostname() + ":" + dataNodeId.getPort());
    snapshot.put(REPLICA_PARTITION, getPartitionId().toPathString());
    snapshot.put(REPLICA_DISK, getDiskId().getMountPath());
    snapshot.put(REPLICA_PATH, getReplicaPath());
    snapshot.put(CAPACITY_BYTES, getCapacityInBytes());
    snapshot.put(REPLICA_WRITE_STATE, isSealed() ? PartitionState.READ_ONLY.name() : PartitionState.READ_WRITE.name());
    String replicaLiveness = UP;
    if (dataNodeId.getState() == HardwareState.UNAVAILABLE) {
      replicaLiveness = NODE_DOWN;
    } else if (disk.getState() == HardwareState.UNAVAILABLE) {
      replicaLiveness = DISK_DOWN;
    } else if (resourceStatePolicy.isDown()) {
      replicaLiveness = SOFT_DOWN;
    } else if (isStopped) {
      replicaLiveness = REPLICA_STOPPED;
    }
    snapshot.put(LIVENESS, replicaLiveness);
    return snapshot;
  }

  @Override
  public void markDiskDown() {
    disk.onDiskError();
  }

  @Override
  public void markDiskUp() {
    disk.onDiskOk();
  }

  @Override
  public ReplicaType getReplicaType() {
    return replicaType;
  }

  public ClusterMapConfig getClusterMapConfig() {
    return clusterMapConfig;
  }

  Partition getPartition() {
    return partition;
  }

  List<ReplicaId> getPeerReplicas() {
    List<ReplicaId> peers = new ArrayList<>(partition.getReplicaIds().size());
    for (ReplicaId peer : partition.getReplicas()) {
      if (!peer.equals(this)) {
        peers.add(peer);
      }
    }
    return peers;
  }

  protected void validatePartition() {
    if (partition == null) {
      throw new IllegalStateException("Partition cannot be null.");
    }
  }

  private void validateDisk() {
    if (disk == null) {
      throw new IllegalStateException("Disk cannot be null.");
    }
  }

  private void validate() {
    logger.trace("begin validate.");
    validatePartition();
    validateDisk();
    logger.trace("complete validate.");
  }

  JSONObject toJSONObject() throws JSONException {
    // Effectively serializes the "foreign key" into hardwareLayout to find Disk.
    return new JSONObject().put("hostname", disk.getDataNode().getHostname())
        .put("port", disk.getDataNode().getPort())
        .put("mountPath", disk.getMountPath());
  }

  @Override
  public String toString() {
    return "Replica[" + getDataNodeId().getHostname() + ":" + getDataNodeId().getPort() + ":" + getReplicaPath() + "]";
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
