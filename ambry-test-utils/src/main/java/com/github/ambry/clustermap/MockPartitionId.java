/*
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.json.JSONArray;
import org.json.JSONObject;

import static com.github.ambry.clustermap.ClusterMapSnapshotConstants.*;


/**
 * Mock partition id for unit tests
 */
public class MockPartitionId implements PartitionId {

  final Long partition;
  public List<ReplicaId> replicaIds;
  public Map<ReplicaId, ReplicaState> replicaAndState;
  private PartitionState partitionState = PartitionState.READ_WRITE;
  private final String partitionClass;

  public MockPartitionId() {
    this(0L, MockClusterMap.DEFAULT_PARTITION_CLASS);
  }

  public MockPartitionId(long partition, String partitionClass) {
    this.partition = partition;
    this.partitionClass = partitionClass;
    replicaIds = new ArrayList<>(0);
    replicaAndState = new HashMap<>();
  }

  public MockPartitionId(long partition, String partitionClass, List<MockDataNodeId> dataNodes,
      int mountPathIndexToUse) {
    this.partition = partition;
    this.partitionClass = partitionClass;
    this.replicaIds = new ArrayList<>(dataNodes.size());
    replicaAndState = new HashMap<>();
    Set<String> dataCenters = new HashSet<>();
    for (MockDataNodeId dataNode : dataNodes) {
      MockReplicaId replicaId = new MockReplicaId(dataNode.getPort(), this, dataNode, mountPathIndexToUse);
      replicaIds.add(replicaId);
      if (dataCenters.contains(dataNode.getDatacenterName())) {
        replicaAndState.put(replicaId, ReplicaState.STANDBY);
      } else {
        dataCenters.add(dataNode.getDatacenterName());
        replicaAndState.put(replicaId, ReplicaState.LEADER);
      }
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
    return new ArrayList<>(replicaIds);
  }

  @Override
  public List<ReplicaId> getReplicaIdsByState(ReplicaState state, String dcName) {
    return replicaIds.stream()
        .filter(r -> replicaAndState.get(r) == state && (dcName == null || r.getDataNodeId()
            .getDatacenterName()
            .equals(dcName)))
        .collect(Collectors.toList());
  }

  @Override
  public PartitionState getPartitionState() {
    return partitionState;
  }

  @Override
  public int compareTo(PartitionId o) {
    MockPartitionId mockPartition = (MockPartitionId) o;
    return Long.compare(partition, mockPartition.partition);
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
   * Set state of this partition.
   * @param state the {@link PartitionState} associated with this partition.
   */
  public void setPartitionState(PartitionState state) {
    partitionState = state;
  }

  /**
   * If all replicaIds == !isSealed, then partition status = Read-Write, else Read-Only
   */
  public void resolvePartitionStatus() {
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

  @Override
  public String getPartitionClass() {
    return partitionClass;
  }

  @Override
  public JSONObject getSnapshot() {
    JSONObject snapshot = new JSONObject();
    snapshot.put(PARTITION_ID, partition);
    snapshot.put(PARTITION_WRITE_STATE, partitionState.name());
    snapshot.put(PARTITION_CLASS, partitionClass);
    JSONArray replicas = new JSONArray();
    for (ReplicaId replicaId : replicaIds) {
      replicas.put(replicaId.getSnapshot());
    }
    snapshot.put(PARTITION_REPLICAS, replicas);
    return snapshot;
  }

  public void cleanUp() {
    for (ReplicaId replicaId : replicaIds) {
      ((MockReplicaId) replicaId).cleanup();
    }
    replicaIds.clear();
    replicaAndState.clear();
  }

  public void onPartitionReadOnly() {
    /* noop for now */
  }
}
