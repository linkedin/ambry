/*
 * Copyright 2026 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.replication;

import com.github.ambry.clustermap.PartitionId;


/**
 * A single {@code (partition, boost, isInterColo)} entry in a replication-priority snapshot.
 *
 * <p>{@code boost} is the fetchSize weight currently applied to this partition: each cycle the
 * thread divides its total fetch budget in proportion to per-replica weights, so a partition
 * with {@code boost=N} gets roughly {@code N}× the per-cycle bytes of a normal replica
 * (implicit weight {@code 1}). {@code isInterColo=true} means the entry came from a
 * cross-datacenter ReplicaThread pool; {@code false} means the intra-datacenter pool.
 *
 * <p>Lives in {@code ambry-api} so the {@link ReplicationAPI#listAllPriorityPartitions()} default
 * can return {@code List<PriorityEntry>} without {@code ambry-api} taking a dependency on
 * {@code ambry-protocol}.
 */
public class PriorityEntry {
  private final PartitionId partitionId;
  private final int boost;
  private final boolean isInterColo;

  public PriorityEntry(PartitionId partitionId, int boost, boolean isInterColo) {
    this.partitionId = partitionId;
    this.boost = boost;
    this.isInterColo = isInterColo;
  }

  public PartitionId getPartitionId() {
    return partitionId;
  }

  public int getBoost() {
    return boost;
  }

  public boolean isInterColo() {
    return isInterColo;
  }

  @Override
  public String toString() {
    return "PriorityEntry[" + partitionId + ", boost=" + boost + ", isInterColo=" + isInterColo + "]";
  }
}
