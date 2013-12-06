package com.github.ambry.clustermap;

import java.util.List;

/**
 * A PartitionId consists of one or more {@link ReplicaId}s. A PartitionId is uniquely identified by an ID.
 */
public abstract class PartitionId implements Comparable<PartitionId> {
  /**
   * Serializes the ID of this PartitionId to bytes.
   *
   * @return byte-serialized ID of this PartitionId.
   */
  public abstract byte[] getBytes();

  /**
   * Gets Replicas that comprise this PartitionId.
   *
   * @return list of the Replicas that comprise this PartitionId.
   */
  public abstract List<ReplicaId> getReplicaIds();

  /**
   * Gets the state of this PartitionId.
   *
   * @return state of this PartitionId.
   */
  public abstract PartitionState getPartitionState();
}
