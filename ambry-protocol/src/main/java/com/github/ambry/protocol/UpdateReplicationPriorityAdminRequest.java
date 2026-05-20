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
package com.github.ambry.protocol;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


/**
 * Admin request that sets, unsets, or wipes-all replication priority for partitions on a single
 * storage node.
 *
 * <p>{@code boost} is an integer weight applied to the prioritized partition's per-iteration
 * fetchSize. Each cycle, the thread's total fetch budget is divided among all replicas in
 * proportion to their weights: a non-priority replica has weight {@code 1}, so a partition set
 * with {@code boost=N} gets roughly {@code N}× the per-cycle bytes of a normal replica. The
 * server rejects {@code boost < 1} on the {@link Action#SET} path.
 *
 * <p>Wire semantics — exactly one {@link Action} per request; the server rejects any partition-list
 * shape that does not match the action:
 * <ul>
 *   <li>{@link Action#SET} (non-empty partitions): set each listed partition's boost to {@code
 *       boost}.</li>
 *   <li>{@link Action#UNSET} (non-empty partitions): remove only the listed partitions from the
 *       priority table.</li>
 *   <li>{@link Action#UNSET_ALL} (empty partitions): wipe ALL priority entries on this host.</li>
 * </ul>
 * The split between {@link Action#UNSET} and {@link Action#UNSET_ALL} is explicit so an operator
 * typo that sends an empty partition list cannot accidentally wipe everything.
 */
public class UpdateReplicationPriorityAdminRequest extends AdminRequest {
  private static final short VERSION_V1 = 1;
  // Wire-level sanity bound; handler enforces a tighter operator-facing cap.
  private static final int MAX_PARTITIONS_ON_WIRE = 10000;

  /** The action this request performs. Append only — wire encoding is ordinal-based. */
  public enum Action {
    SET, UNSET, UNSET_ALL
  }

  private final List<PartitionId> partitionIds;
  private final int boost;
  private final Action action;
  private final long sizeInBytes;

  /**
   * Reads from a stream and constructs a {@link UpdateReplicationPriorityAdminRequest}.
   * @param stream the stream to read from
   * @param clusterMap the cluster map used to resolve partition ids
   * @param adminRequest the {@link AdminRequest} containing common header fields
   * @return the {@link UpdateReplicationPriorityAdminRequest} constructed from {@code stream}
   * @throws IOException if there is any problem reading from the stream
   */
  public static UpdateReplicationPriorityAdminRequest readFrom(DataInputStream stream, ClusterMap clusterMap,
      AdminRequest adminRequest) throws IOException {
    Short versionId = stream.readShort();
    if (!versionId.equals(VERSION_V1)) {
      throw new IllegalStateException("Unrecognized version for UpdateReplicationPriorityAdminRequest: " + versionId);
    }
    Action action = Action.values()[stream.readShort()];
    int boost = stream.readInt();
    int numPartitions = stream.readInt();
    if (numPartitions < 0 || numPartitions > MAX_PARTITIONS_ON_WIRE) {
      throw new IOException("UpdateReplicationPriorityAdminRequest numPartitions out of range: " + numPartitions);
    }
    List<PartitionId> partitionIds = new ArrayList<>(numPartitions);
    for (int i = 0; i < numPartitions; i++) {
      PartitionId partitionId = clusterMap.getPartitionIdFromStream(stream);
      if (partitionId == null) {
        throw new IOException("Failed to resolve partition id at index " + i);
      }
      partitionIds.add(partitionId);
    }
    return new UpdateReplicationPriorityAdminRequest(partitionIds, boost, action, adminRequest);
  }

  /**
   * @param partitionIds partitions targeted by the operation
   * @param boost fetchSize weight applied to each listed partition on the {@link Action#SET} path
   *              (must be {@code >= 1}; normal replicas have implicit weight {@code 1}); ignored
   *              on the {@link Action#UNSET} / {@link Action#UNSET_ALL} paths
   * @param action the {@link Action} to perform; the partition-list shape must match (see class
   *               javadoc)
   * @param adminRequest the parent {@link AdminRequest} for common header fields
   */
  public UpdateReplicationPriorityAdminRequest(List<PartitionId> partitionIds, int boost, Action action,
      AdminRequest adminRequest) {
    super(AdminRequestOrResponseType.UpdateReplicationPriority, adminRequest.getPartitionId(),
        adminRequest.getCorrelationId(), adminRequest.getClientId());
    this.partitionIds = partitionIds;
    this.boost = boost;
    this.action = action;
    sizeInBytes = computeSizeInBytes();
  }

  public List<PartitionId> getPartitionIds() {
    return Collections.unmodifiableList(partitionIds);
  }

  public int getBoost() {
    return boost;
  }

  public Action getAction() {
    return action;
  }

  @Override
  public long sizeInBytes() {
    return sizeInBytes;
  }

  @Override
  public String toString() {
    return "UpdateReplicationPriorityAdminRequest[ClientId=" + clientId + ", CorrelationId=" + correlationId + ", Action="
        + action + ", Boost=" + boost + ", Partitions=" + partitionIds + "]";
  }

  @Override
  protected void prepareBuffer() {
    super.prepareBuffer();
    bufferToSend.writeShort(VERSION_V1);
    bufferToSend.writeShort((short) action.ordinal());
    bufferToSend.writeInt(boost);
    bufferToSend.writeInt(partitionIds.size());
    for (PartitionId partitionId : partitionIds) {
      bufferToSend.writeBytes(partitionId.getBytes());
    }
  }

  private long computeSizeInBytes() {
    // parent size + version + action + boost + num-partitions
    long size = super.sizeInBytes() + Short.BYTES + Short.BYTES + Integer.BYTES + Integer.BYTES;
    for (PartitionId partitionId : partitionIds) {
      size += partitionId.getBytes().length;
    }
    return size;
  }
}
