package com.github.ambry.clustermap;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;

/**
 * The ClusterMap provides a high-level interface to {@link DataNodeId}s, {@link PartitionId}s and {@link ReplicaId}s.
 */
public interface ClusterMap {

  /**
   * Gets PartitionId based on serialized bytes.
   * @param stream data input stream that contains the serialized partition bytes
   * @return deserialized PartitionId
   */
  public PartitionId getPartitionIdFromStream(DataInputStream stream) throws IOException;

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
   * Checks if datacenter name corresponds to some datacenter in this cluster map's hardware layout.
   *
   * @param datacenterName name of datacenter
   * @return true if datacenter with datacenterName is in the hardware layout of this cluster map.
   */
  public boolean hasDatacenter(String datacenterName);

  /**
   * Gets a specific DataNodeId by its hostname and port.
   *
   * @param hostname of the DataNodeId
   * @param port of the DataNodeId
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
