package com.github.ambry.clustermap;

import java.util.List;

/**
 * A ReplicaId is part of some {@link PartitionId}. The durable state of a ReplicaId is stored in a specific path ("replica
 * path") on a specific device (identified by its "mount path") on a {@link DataNodeId}.
 */
public interface ReplicaId {
  // TODO: Does ReplicaId need a getHardwareState() and/or getPartitionState() method? Should ReplicaId extend
  // PartitionId interface?

  /**
   * Gets the PartitionId of which this ReplicaId is a member.
   *
   * @return PartitionId of which this ReplicaId is a member.
   */
  public PartitionId getPartitionId();

  /**
   * Gets the DataNodeId that stores this ReplicaId.
   *
   * @return DataNodeId that stores this ReplicaId.
   */
  public DataNodeId getDataNodeId();

  /**
   * Gets the mount path to the device that stores this ReplicaId.
   *
   * @return mount path on the DataNodeId to the device that stores this ReplicaId.
   */
  public String getMountPath();

  /**
   * Gets the path to the data for this ReplicaId on the DataNodeId. This path consists of the mount path to the device that
   * stores this ReplicaId followed by a unique path on that device for this ReplicaId.
   *
   * @return replica path to this ReplicaId's state on the DataNodeId.
   */
  public String getReplicaPath();

  /**
   * Gets list of this ReplicaId's peers. The peers of a ReplicaId are the other Replicas with which this replica forms
   * a PartitionId.
   *
   * @return list of the peers of this ReplicaId.
   */
  public List<? extends ReplicaId> getPeerReplicaIds();


}
