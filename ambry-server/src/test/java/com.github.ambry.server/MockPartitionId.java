package com.github.ambry.server;

import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.PartitionState;
import com.github.ambry.clustermap.ReplicaId;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Mock partition id for unit tests
 */
public class MockPartitionId extends PartitionId {

  private Long partition = 1L;
  public MockReplicaId replicaId;

  public MockPartitionId() {
    this.replicaId = new MockReplicaId();
  }

  @Override
  public byte[] getBytes() {
    ByteBuffer buf = ByteBuffer.allocate(8);
    buf.putLong(partition);
    return buf.array();
  }

  @Override
  public List<ReplicaId> getReplicaIds() {
    ArrayList<ReplicaId> replicaIds = new ArrayList<ReplicaId>();
    replicaIds.add(replicaId);
    return replicaIds;
  }

  @Override
  public PartitionState getPartitionState() {
    return PartitionState.READ_WRITE;
  }

  @Override
  public int compareTo(PartitionId o) {
    return 0;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    MockPartitionId mockPartition = (MockPartitionId)o;

    if (partition != mockPartition.partition) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return (int)(partition ^ (partition >>> 32));
  }

  @Override
  public String toString() {
    return partition.toString();
  }
}
