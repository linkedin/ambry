package com.github.ambry.clustermap;

import java.util.List;

/**
 * The ClusterMap provides a high level interface to Partitions and Replicas. A Partition consists of one or more
 * Replicas. A Partition is uniquely identified by a PartitionIdentifier. A Replica is hosted on some DataNode which is
 * identified by its hostname and port. On that DataNode, the Replica is stored in some path identified by the
 * mountPath. The ReplicaContext permits access to these key attributes of a Replica.
 */
public interface ClusterMap {

  /**
   * Find out how many writable partitions there are.
   * @return number of writable partitions.
   */
  public int getNumWritablePartitions();

  /**
   * Fetch PartitionIdentifier of specific Partition.
   * @param offset of Partition of interest.
   * @return PartitionIdentifier of writable Partition at specified offset.
   */
  public PartitionIdentifier getWritablePartitionByOffset(int offset);

  /**
   * Construct a PartitionIdentifier from serialized bytes.
   * @param bytes serialized bytes.
   * @return PartitionIdentifier constructed by deserialization of bytes.
   */
  public PartitionIdentifier getPartitionIdentifier(byte[] bytes);

  /**
   * Fetch a Partition's Replicas.
   * @param partitionIdentifier  of Partition of interest.
   * @return Partition's Replicas in form of ReplicaContexts.
   */
  public List<ReplicaContext> getReplicas(PartitionIdentifier partitionIdentifier);

  /**
   * Fetch a Replica's peers.
   * @param partitionIdentifier of Partition.
   * @param replicaContext of Replica.
   * @return Peer Replicas in form of ReplicaContexts.
   */
  public List<ReplicaContext> getPeerReplicas(PartitionIdentifier partitionIdentifier, ReplicaContext replicaContext);
}
