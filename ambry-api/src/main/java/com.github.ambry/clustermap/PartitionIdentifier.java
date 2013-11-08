package com.github.ambry.clustermap;

/**
 * The PartitionIdentifier uniquely identifies a Partition. A PartitionIdentifier is used in the ClusterMap interface to
 * get properties of the Partition.
 * <p/>
 * PartitionIdentifier provides an interface to serialize to bytes for sake of including PartitionIdentifier in BlobId.
 */
public interface PartitionIdentifier {
  /**
   * Serialization method for including PartitionIdentifier in types such as BlobId.
   *
   * @return byte array of serialized PartitionIdentifier.
   */
  public byte[] getBytes();
}
