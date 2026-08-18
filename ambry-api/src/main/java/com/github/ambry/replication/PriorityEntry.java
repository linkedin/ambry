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
  private final String threadName;

  /**
   * @param partitionId the prioritized partition.
   * @param boost the fetchSize weight applied to the partition.
   * @param isInterColo whether the entry came from the inter-colo (cross-datacenter) thread pool.
   * @param threadName the {@link ReplicaThread} holding this entry; may be {@code null} if unknown.
   */
  public PriorityEntry(PartitionId partitionId, int boost, boolean isInterColo, String threadName) {
    this.partitionId = partitionId;
    this.boost = boost;
    this.isInterColo = isInterColo;
    this.threadName = threadName;
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

  /**
   * @return the name of the {@link ReplicaThread} that holds this priority entry, or {@code null} if unknown.
   *         Two threads on the same node can each report the same partition (one entry per thread), so this
   *         disambiguates otherwise-identical {@code (partition, boost, isInterColo)} rows.
   */
  public String getThreadName() {
    return threadName;
  }

  @Override
  public String toString() {
    return "PriorityEntry[" + partitionId + ", boost=" + boost + ", isInterColo=" + isInterColo + ", threadName="
        + threadName + "]";
  }
}
