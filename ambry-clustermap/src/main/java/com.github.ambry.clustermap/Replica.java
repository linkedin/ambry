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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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

  private Logger logger = LoggerFactory.getLogger(getClass());

  Replica(Partition partition, Disk disk) {
    if (logger.isTraceEnabled()) {
      logger.trace("Replica " + partition + ", " + disk);
    }
    this.partition = partition;
    this.disk = disk;

    validate();
  }

  Replica(HardwareLayout hardwareLayout, Partition partition, JSONObject jsonObject) throws JSONException {
    this.partition = partition;
    this.disk = hardwareLayout.findDisk(jsonObject.getString("hostname"), jsonObject.getInt("port"),
        jsonObject.getString("mountPath"));
    validate();
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
    List<Replica> peerReplicas = getPeerReplicas();
    return new ArrayList<ReplicaId>(peerReplicas);
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
        || getDiskId().getState() == HardwareState.UNAVAILABLE;
  }

  @Override
  public boolean isSealed() {
    return partition.getPartitionState().equals(PartitionState.READ_ONLY);
  }

  Partition getPartition() {
    return partition;
  }

  List<Replica> getPeerReplicas() {
    List<Replica> peers = new ArrayList<Replica>(partition.getReplicas().size());
    for (Replica peer : partition.getReplicas()) {
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
}
