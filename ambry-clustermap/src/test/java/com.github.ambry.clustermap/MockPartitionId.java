/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
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
  private PartitionState partitionState = PartitionState.READ_WRITE;

  public MockPartitionId() {
    this(0L);
  }

  public MockPartitionId(long partition) {
    this.partition = partition;
    replicaIds = new ArrayList<>(0);
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
    return partitionState;
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

  /**
   * If all replicaIds == !isSealed, then partition status = Read-Write, else Read-Only
   */
  void resolvePartitionStatus() {
    boolean isReadWrite = true;
    for (ReplicaId replicaId : replicaIds) {
      if (replicaId.isSealed()) {
        isReadWrite = false;
        break;
      }
    }
    partitionState = isReadWrite ? PartitionState.READ_WRITE : PartitionState.READ_ONLY;
  }

  @Override
  public int hashCode() {
    return (int) (partition ^ (partition >>> 32));
  }

  @Override
  public String toString() {
    return partition.toString();
  }

  @Override
  public String toPathString() {
    return String.valueOf(partition);
  }

  public void cleanUp() {
    for (ReplicaId replicaId : replicaIds) {
      ((MockReplicaId) replicaId).cleanup();
    }
  }

  public void onPartitionReadOnly() {
    /* noop for now */
  }
}
