/*
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
import java.util.Objects;
import org.json.JSONObject;

import static com.github.ambry.clustermap.ClusterMapSnapshotConstants.*;


public class MockReplicaId implements ReplicaId {
  public static final long MOCK_REPLICA_CAPACITY = 100000000;
  private static final String REPLICA_FILE_PREFIX = "replica";
  private String mountPath;
  private String replicaPath;
  private List<ReplicaId> peerReplicas;
  private MockPartitionId partitionId;
  private MockDataNodeId dataNodeId;
  private MockDiskId diskId;
  private boolean isMarkedDown = false;
  private ReplicaType replicaType;
  private volatile boolean isSealed;

  public MockReplicaId(ReplicaType replicaType) {
    this.replicaType = replicaType;
  }

  public MockReplicaId(int port, MockPartitionId partitionId, MockDataNodeId dataNodeId, int indexOfMountPathToUse) {
    this.partitionId = partitionId;
    this.dataNodeId = dataNodeId;
    if (dataNodeId.getMountPaths().isEmpty()) {
      // a data node with no mount paths is a virtual data node which holds cloud service replicas.
      replicaType = ReplicaType.CLOUD_BACKED;
    } else {
      mountPath = dataNodeId.getMountPaths().get(indexOfMountPathToUse);
      File mountFile = new File(mountPath);
      File replicaFile = new File(mountFile, REPLICA_FILE_PREFIX + port + partitionId.partition);
      replicaFile.mkdir();
      replicaFile.deleteOnExit();
      if (mountPath.startsWith(CLOUD_REPLICA_MOUNT)) {
        replicaType = ReplicaType.CLOUD_BACKED;
      } else {
        replicaType = ReplicaType.DISK_BACKED;
      }
      replicaPath = replicaFile.getAbsolutePath();
      diskId = new MockDiskId(dataNodeId, mountPath);
    }
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
    this.peerReplicas = new ArrayList<>();
    for (ReplicaId replicaId : peerReplicas) {
      if (!Objects.equals(mountPath, replicaId.getMountPath())) {
        this.peerReplicas.add(replicaId);
      }
    }
  }

  @Override
  public long getCapacityInBytes() {
    return MOCK_REPLICA_CAPACITY;
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
    return isMarkedDown || getDataNodeId().getState() == HardwareState.UNAVAILABLE || (getDiskId() != null
        && getDiskId().getState() == HardwareState.UNAVAILABLE);
  }

  @Override
  public boolean isSealed() {
    return isSealed;
  }

  @Override
  public JSONObject getSnapshot() {
    JSONObject snapshot = new JSONObject();
    snapshot.put(REPLICA_NODE, dataNodeId.getHostname() + ":" + dataNodeId.getPort());
    snapshot.put(REPLICA_PARTITION, partitionId.toPathString());
    if (diskId != null) {
      snapshot.put(REPLICA_DISK, diskId.getMountPath());
    }
    snapshot.put(REPLICA_PATH, replicaPath);
    snapshot.put(REPLICA_WRITE_STATE, isSealed() ? PartitionState.READ_ONLY.name() : PartitionState.READ_WRITE.name());
    String liveness = UP;
    if (isMarkedDown) {
      liveness = DOWN;
    } else if (getDataNodeId().getState() == HardwareState.UNAVAILABLE) {
      liveness = NODE_DOWN;
    } else if (getDiskId() != null && getDiskId().getState() == HardwareState.UNAVAILABLE) {
      liveness = DISK_DOWN;
    }
    snapshot.put(LIVENESS, liveness);
    return snapshot;
  }

  public void setSealedState(boolean isSealed) {
    this.isSealed = isSealed;
    partitionId.resolvePartitionStatus();
  }

  @Override
  public String toString() {
    return "Mount Path " + mountPath + " Replica Path " + replicaPath;
  }

  @Override
  public void markDiskDown() {
    diskId.onDiskError();
  }

  @Override
  public void markDiskUp() {
    diskId.onDiskOk();
  }

  @Override
  public ReplicaType getReplicaType() {
    return replicaType;
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
