package com.github.ambry.clustermap;

import com.github.ambry.config.ClusterMapConfig;
import org.json.JSONObject;

import static com.github.ambry.clustermap.ClusterMapSnapshotConstants.*;
import static com.github.ambry.clustermap.ClusterMapUtils.*;


class CloudAmbryReplica extends AmbryReplica {
  /**
   * Instantiate an AmbryReplica instance for a virtual cloud replica. This does no
   * @param clusterMapConfig the {@link ClusterMapConfig} to use.
   * @param partition the {@link AmbryPartition} of which this is a replica.
   * @param capacityBytes the capacity in bytes for this replica.
   */
  CloudAmbryReplica(ClusterMapConfig clusterMapConfig, AmbryPartition partition, long capacityBytes) throws Exception {
    super(clusterMapConfig, partition, false, capacityBytes, false);
  }

  @Override
  public AmbryDisk getDiskId() {
    throw new UnsupportedOperationException("No disk for cloud replica");
  }

  @Override
  public AmbryDataNode getDataNodeId() {
    // TODO figure out callers, if too many change this behavior
    throw new UnsupportedOperationException("No datanode for cloud replica");
  }

  @Override
  public String getMountPath() {
    throw new UnsupportedOperationException("No mount path for cloud replica");
  }

  @Override
  public String getReplicaPath() {
    throw new UnsupportedOperationException("No replica path for cloud replica");
  }

  @Override
  public ReplicaType getReplicaType() {
    return ReplicaType.DISK_BACKED;
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
  }

  @Override
  public void markDiskUp() {
  }
}
