package com.github.ambry.clustermap;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Mock partition id for unit tests
 */
public class MockPartitionId extends PartitionId {

  long partition = 1;

  @Override
  public byte[] getBytes() {
    ByteBuffer buf = ByteBuffer.allocate(8);
    buf.putLong(partition);
    return buf.array();
  }

  @Override
  public List<ReplicaId> getReplicaIds() {
    return null;
  }

  @Override
  public PartitionState getPartitionState() {
    return PartitionState.READ_WRITE;
  }

  @Override
  public int compareTo(PartitionId o) {
    return 0;
  }
}
