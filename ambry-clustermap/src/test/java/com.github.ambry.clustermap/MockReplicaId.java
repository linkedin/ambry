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


public class MockReplicaId implements ReplicaId {

  private String mountPath;
  private String replicaPath;
  private List<ReplicaId> peerReplicas;
  private MockPartitionId partitionId;
  private MockDataNodeId dataNodeId;
  private MockDiskId diskId;
  private boolean isMarkedDown = false;
  private volatile boolean isSealed;

  public MockReplicaId() {
  }

  public MockReplicaId(int port, MockPartitionId partitionId, MockDataNodeId dataNodeId, int indexOfMountPathToUse) {
    this.partitionId = partitionId;
    this.dataNodeId = dataNodeId;
    mountPath = dataNodeId.getMountPaths().get(indexOfMountPathToUse);
    File mountFile = new File(mountPath);
    File replicaFile = new File(mountFile, "replica" + port + ((MockPartitionId) partitionId).partition);
    replicaFile.mkdir();
    replicaFile.deleteOnExit();
    replicaPath = replicaFile.getAbsolutePath();
    diskId = new MockDiskId(dataNodeId, mountPath);
    isSealed = partitionId.getPartitionState().equals(PartitionState.READ_ONLY);
  }

  @Override
  public PartitionId getPartitionId() {
    return partitionId;
  }

  @Override
  public DataNodeId getDataNodeId() {
    return dataNodeId;
  }

  @Override
  public String getMountPath() {
    return mountPath;
  }

  @Override
  public String getReplicaPath() {
    return replicaPath;
  }

  @Override
  public List<ReplicaId> getPeerReplicaIds() {
    return peerReplicas;
  }

  public void setPeerReplicas(List<ReplicaId> peerReplicas) {
    this.peerReplicas = new ArrayList<ReplicaId>();
    for (ReplicaId replicaId : peerReplicas) {
      if (!(replicaId.getMountPath().compareTo(mountPath) == 0)) {
        this.peerReplicas.add(replicaId);
      }
    }
  }

  @Override
  public long getCapacityInBytes() {
    return 100000000;
  }

  @Override
  public DiskId getDiskId() {
    return diskId;
  }

  /**
   * @return true if the replica is down; false otherwise.
   */
  @Override
  public boolean isDown() {
    return isMarkedDown || getDataNodeId().getState() == HardwareState.UNAVAILABLE
        || getDiskId().getState() == HardwareState.UNAVAILABLE;
  }

  @Override
  public boolean isSealed() {
    return isSealed;
  }

  public void setSealedState(boolean isSealed) {
    this.isSealed = isSealed;
    partitionId.resolvePartitionStatus();
  }

  @Override
  public String toString() {
    return "Mount Path " + mountPath + " Replica Path " + replicaPath;
  }

  public void cleanup() {
    File replicaDir = new File(replicaPath);
    File[] replicaDirFiles = replicaDir.listFiles();
    if (replicaDirFiles != null) {
      for (File replica : replicaDirFiles) {
        replica.delete();
      }
    }
    replicaDir.delete();
  }

  /**
   * Mark a replica as down or up.
   * @param isDown if true, marks the replica as down; if false, marks it as up.
   */
  public void markReplicaDownStatus(boolean isDown) {
    isMarkedDown = isDown;
  }
}
