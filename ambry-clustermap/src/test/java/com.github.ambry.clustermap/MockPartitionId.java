package com.github.ambry.clustermap;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


/**
 * Mock partition id for unit tests
 */
public class MockPartitionId extends PartitionId {

  Long partition;
  public List<ReplicaId> replicaIds;

  public MockPartitionId() {
    partition = 0L;
    replicaIds = new ArrayList<ReplicaId>(0);
  }

  public MockPartitionId(long partition, List<MockDataNodeId> dataNodes, int mountPathIndexToUse) {
    this.partition = partition;

    this.replicaIds = new ArrayList<ReplicaId>(dataNodes.size());
    for (MockDataNodeId dataNode : dataNodes) {
      MockReplicaId replicaId = new MockReplicaId(dataNode.getPort(), this, dataNode, mountPathIndexToUse);
      replicaIds.add(replicaId);
    }
    for (ReplicaId replicaId : replicaIds) {
      ((MockReplicaId) replicaId).setPeerReplicas(replicaIds);
    }
  }

  @Override
  public byte[] getBytes() {
    ByteBuffer buf = ByteBuffer.allocate(10);
    buf.putShort((short) 1);
    buf.putLong(partition);
    return buf.array();
  }

  @Override
  public List<ReplicaId> getReplicaIds() {
    return replicaIds;
  }

  @Override
  public PartitionState getPartitionState() {
    return PartitionState.READ_WRITE;
  }

  @Override
  public int compareTo(PartitionId o) {
    MockPartitionId mockPartition = (MockPartitionId) o;
    return (partition < mockPartition.partition) ? -1 : ((partition == mockPartition.partition) ? 0 : 1);
  }

  @Override
  public boolean isEqual(String partitionId) {
    return partition.toString().equals(partitionId);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MockPartitionId mockPartition = (MockPartitionId) o;

    if (partition != mockPartition.partition) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return (int) (partition ^ (partition >>> 32));
  }

  @Override
  public String toString() {
    return partition.toString();
  }

  public void cleanUp() {
    for (ReplicaId replicaId : replicaIds) {
      ((MockReplicaId) replicaId).cleanup();
    }
  }

  @Override
  public void onPartitionReadOnly() {
    /* noop for now */
  }
}
