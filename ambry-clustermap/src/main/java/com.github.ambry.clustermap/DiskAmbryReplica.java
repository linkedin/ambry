package com.github.ambry.clustermap;

import com.github.ambry.config.ClusterMapConfig;
import java.io.File;
import java.util.Objects;
import org.json.JSONObject;

import static com.github.ambry.clustermap.ClusterMapSnapshotConstants.*;


class DiskAmbryReplica extends AmbryReplica {
  private final AmbryDisk disk;

  /**
   * Instantiate a disk backed AmbryReplica instance.
   * @param clusterMapConfig the {@link ClusterMapConfig} to use.
   * @param partition the {@link AmbryPartition} of which this is a replica.
   * @param disk the {@link AmbryDisk} on which this replica resides.
   * @param isReplicaStopped whether this replica is stopped or not.
   * @param capacityBytes the capacity in bytes for this replica.
   * @param isSealed whether this replica is in sealed state.
   */
  DiskAmbryReplica(ClusterMapConfig clusterMapConfig, AmbryPartition partition, AmbryDisk disk,
      boolean isReplicaStopped, long capacityBytes, boolean isSealed) throws Exception {
    super(clusterMapConfig, partition, isReplicaStopped, capacityBytes, isSealed);
    this.disk = Objects.requireNonNull(disk, "null disk");
  }

  @Override
  public AmbryDisk getDiskId() {
    return disk;
  }

  @Override
  public AmbryDataNode getDataNodeId() {
    return disk != null ? disk.getDataNode() : null;
  }

  @Override
  public String getMountPath() {
    return disk.getMountPath();
  }

  @Override
  public String getReplicaPath() {
    return disk.getMountPath() + File.separator + getPartitionId().toPathString();
  }

  @Override
  public ReplicaType getReplicaType() {
    return ReplicaType.DISK_BACKED;
  }

  @Override
  public JSONObject getSnapshot() {
    JSONObject snapshot = new JSONObject();
    DataNodeId dataNodeId = getDataNodeId();
    snapshot.put(REPLICA_NODE, dataNodeId.getHostname() + ":" + dataNodeId.getPort());
    snapshot.put(REPLICA_PARTITION, getPartitionId().toPathString());
    snapshot.put(REPLICA_TYPE, getReplicaType());
    snapshot.put(REPLICA_DISK, getDiskId().getMountPath());
    snapshot.put(REPLICA_PATH, getReplicaPath());
    snapshot.put(CAPACITY_BYTES, getCapacityInBytes());
    snapshot.put(REPLICA_WRITE_STATE, isSealed() ? PartitionState.READ_ONLY.name() : PartitionState.READ_WRITE.name());
    String replicaLiveness = UP;
    if (dataNodeId.getState() == HardwareState.UNAVAILABLE) {
      replicaLiveness = NODE_DOWN;
    } else if (disk.getState() == HardwareState.UNAVAILABLE) {
      replicaLiveness = DISK_DOWN;
    } else if (isStopped) {
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
    return "Replica[" + getDataNodeId().getHostname() + ":" + getDataNodeId().getPort() + ":" + getReplicaPath() + "]";
  }

  @Override
  public boolean isDown() {
    return disk.getState() == HardwareState.UNAVAILABLE || super.isDown();
  }

  @Override
  public void markDiskDown() {
    disk.onDiskError();
  }

  @Override
  public void markDiskUp() {
    disk.onDiskOk();
  }
}
