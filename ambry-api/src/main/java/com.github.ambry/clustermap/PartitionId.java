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

  /**
   * Compares the PartitionId to a string representation of another PartitionId
   * @param partitionId  The string form of the partition that needs to be compared against
   * @return True, if the partitions match, false otherwise
   */
  public abstract boolean isEqual(String partitionId);

  /**
   * Performs the required action when an operation on the associated Partition receives a Partition_ReadOnly error.
   */
  public abstract void onPartitionReadOnly();
}

