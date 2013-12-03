package com.github.ambry.clustermap;

import java.util.List;

/**
 * The ClusterMap provides a high-level interface to {@link DataNodeId}s, {@link PartitionId}s and {@link ReplicaId}s.
 */
public interface ClusterMap {

  /**
   * Gets PartitionId based on serialized bytes.
   *
   * @param bytes byte-serialized PartitionId.
   * @return deserialized PartitionId.
   */
  public PartitionId getPartitionIdFromBytes(byte[] bytes);

  /**
   * Determines count of writable PartitionIds so that an "index" can be used to retrieve a specific such PartitionId.
   *
   * @return count of writable PartitionIds.
   */
  public long getWritablePartitionIdsCount();

  /**
   * Gets a specific writable PartitionId by its index.
   *
   * @param index of desired writable PartitionId.
   * @return specified writable PartitionId.
   */
  public PartitionId getWritablePartitionIdAt(long index);

  /**
   * Gets a specific DataNodeId by its hostname and port.
   *
   * @param hostname of the DataNodeId
   * @param port     of the DataNodeId
   * @return DataNodeId for this hostname and port.
   */
  public DataNodeId getDataNodeId(String hostname, int port);

  /**
   * Gets the ReplicaIds stored on the specified DataNodeId.
   *
   * @param dataNodeId
   * @return list of ReplicaIds on the specified dataNodeId
   */
  public List<ReplicaId> getReplicaIds(DataNodeId dataNodeId);
}
