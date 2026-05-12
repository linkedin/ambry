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
 * Admin request that sets, or clears, replication priority for a set of partitions on a single
 * storage node.
 *
 * <p>{@code boost} is an integer weight applied to the prioritized partition's per-iteration
 * fetchSize. Each cycle, the thread's total fetch budget is divided among all replicas in
 * proportion to their weights: a non-priority replica has weight {@code 1}, so a partition set
 * with {@code boost=N} gets roughly {@code N}× the per-cycle bytes of a normal replica. The
 * server rejects {@code boost < 1} when {@code clear=false}.
 *
 * <p>Wire semantics:
 * <ul>
 *   <li>{@code clear=false}: set each listed partition's boost to {@code boost}.</li>
 *   <li>{@code clear=true} with a non-empty partition list: remove only the listed partitions.</li>
 *   <li>{@code clear=true} with an empty partition list: clear all priorities on this host.
 *       The frontend's request-shape guard prevents the "clear everywhere with no operator
 *       intent" footgun by requiring either {@code servers} or {@code partitions} on the API.</li>
 * </ul>
 */
public class SetReplicationPriorityAdminRequest extends AdminRequest {
  private static final short VERSION_V1 = 1;

  private final List<PartitionId> partitionIds;
  private final int boost;
  private final boolean clear;
  private final long sizeInBytes;

  /**
   * Reads from a stream and constructs a {@link SetReplicationPriorityAdminRequest}.
   * @param stream the stream to read from
   * @param clusterMap the cluster map used to resolve partition ids
   * @param adminRequest the {@link AdminRequest} containing common header fields
   * @return the {@link SetReplicationPriorityAdminRequest} constructed from {@code stream}
   * @throws IOException if there is any problem reading from the stream
   */
  public static SetReplicationPriorityAdminRequest readFrom(DataInputStream stream, ClusterMap clusterMap,
      AdminRequest adminRequest) throws IOException {
    Short versionId = stream.readShort();
    if (!versionId.equals(VERSION_V1)) {
      throw new IllegalStateException("Unrecognized version for SetReplicationPriorityAdminRequest: " + versionId);
    }
    boolean clear = stream.readByte() == 1;
    int boost = stream.readInt();
    int numPartitions = stream.readInt();
    List<PartitionId> partitionIds = new ArrayList<>(numPartitions);
    for (int i = 0; i < numPartitions; i++) {
      partitionIds.add(clusterMap.getPartitionIdFromStream(stream));
    }
    return new SetReplicationPriorityAdminRequest(partitionIds, boost, clear, adminRequest);
  }

  /**
   * @param partitionIds partitions targeted by the operation
   * @param boost fetchSize weight applied to each listed partition when {@code clear} is false
   *              (must be {@code >= 1}; normal replicas have implicit weight {@code 1}); ignored
   *              when {@code clear} is true
   * @param clear if true, remove the listed partitions (or all, if the list is empty); if false, set their boost
   * @param adminRequest the parent {@link AdminRequest} for common header fields
   */
  public SetReplicationPriorityAdminRequest(List<PartitionId> partitionIds, int boost, boolean clear,
      AdminRequest adminRequest) {
    super(AdminRequestOrResponseType.SetReplicationPriority, adminRequest.getPartitionId(),
        adminRequest.getCorrelationId(), adminRequest.getClientId());
    this.partitionIds = partitionIds;
    this.boost = boost;
    this.clear = clear;
    sizeInBytes = computeSizeInBytes();
  }

  public List<PartitionId> getPartitionIds() {
    return Collections.unmodifiableList(partitionIds);
  }

  public int getBoost() {
    return boost;
  }

  public boolean shouldClear() {
    return clear;
  }

  @Override
  public long sizeInBytes() {
    return sizeInBytes;
  }

  @Override
  public String toString() {
    return "SetReplicationPriorityAdminRequest[ClientId=" + clientId + ", CorrelationId=" + correlationId + ", Clear="
        + clear + ", Boost=" + boost + ", Partitions=" + partitionIds + "]";
  }

  @Override
  protected void prepareBuffer() {
    super.prepareBuffer();
    bufferToSend.writeShort(VERSION_V1);
    bufferToSend.writeByte(clear ? (byte) 1 : 0);
    bufferToSend.writeInt(boost);
    bufferToSend.writeInt(partitionIds.size());
    for (PartitionId partitionId : partitionIds) {
      bufferToSend.writeBytes(partitionId.getBytes());
    }
  }

  private long computeSizeInBytes() {
    // parent size + version + clear-byte + boost + num-partitions
    long size = super.sizeInBytes() + Short.BYTES + Byte.BYTES + Integer.BYTES + Integer.BYTES;
    for (PartitionId partitionId : partitionIds) {
      size += partitionId.getBytes().length;
    }
    return size;
  }
}
