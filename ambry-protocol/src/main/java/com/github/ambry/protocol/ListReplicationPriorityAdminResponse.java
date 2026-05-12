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
 * Admin response for {@link ListReplicationPriorityAdminRequest}. Carries a snapshot of
 * priority entries held by every {@link com.github.ambry.replication.ReplicaThread} on the
 * target storage node at handler-invocation time.
 *
 * Each {@link PriorityEntry} carries an {@code isInterColo} flag indicating whether the entry
 * came from the inter-colo (cross-datacenter) replication thread pool or the intra-colo pool.
 * The current design applies priorities to both pools by default, so the same partition
 * typically appears twice in the response — once per pool.
 */
public class ListReplicationPriorityAdminResponse extends AdminResponse {
  private static final short VERSION_V1 = 1;

  private final List<PriorityEntry> entries;
  private final long sizeInBytes;

  public static ListReplicationPriorityAdminResponse readFrom(DataInputStream stream, ClusterMap clusterMap)
      throws IOException {
    AdminResponse adminResponse = AdminResponse.readFrom(stream);
    Short versionId = stream.readShort();
    if (!versionId.equals(VERSION_V1)) {
      throw new IllegalStateException("Unrecognized version for ListReplicationPriorityAdminResponse: " + versionId);
    }
    int numEntries = stream.readInt();
    List<PriorityEntry> entries = new ArrayList<>(numEntries);
    for (int i = 0; i < numEntries; i++) {
      PartitionId partitionId = clusterMap.getPartitionIdFromStream(stream);
      int boost = stream.readInt();
      boolean isInterColo = stream.readByte() != 0;
      entries.add(new PriorityEntry(partitionId, boost, isInterColo));
    }
    return new ListReplicationPriorityAdminResponse(entries, adminResponse);
  }

  public ListReplicationPriorityAdminResponse(List<PriorityEntry> entries, AdminResponse adminResponse) {
    super(adminResponse.getCorrelationId(), adminResponse.getClientId(), adminResponse.getError());
    this.entries = Collections.unmodifiableList(new ArrayList<>(entries));
    this.sizeInBytes = computeSizeInBytes();
  }

  public List<PriorityEntry> getEntries() {
    return entries;
  }

  @Override
  public long sizeInBytes() {
    return sizeInBytes;
  }

  @Override
  public String toString() {
    return "ListReplicationPriorityAdminResponse[ClientId=" + clientId + ", CorrelationId=" + correlationId
        + ", Entries=" + entries + "]";
  }

  @Override
  protected void prepareBuffer() {
    super.prepareBuffer();
    bufferToSend.writeShort(VERSION_V1);
    bufferToSend.writeInt(entries.size());
    for (PriorityEntry entry : entries) {
      bufferToSend.writeBytes(entry.getPartitionId().getBytes());
      bufferToSend.writeInt(entry.getBoost());
      bufferToSend.writeByte(entry.isInterColo() ? (byte) 1 : (byte) 0);
    }
  }

  private long computeSizeInBytes() {
    // parent + version + num-entries
    long size = super.sizeInBytes() + Short.BYTES + Integer.BYTES;
    for (PriorityEntry entry : entries) {
      size += entry.getPartitionId().getBytes().length;  // partition id
      size += Integer.BYTES;                              // boost
      size += Byte.BYTES;                                 // isInterColo flag
    }
    return size;
  }

  /**
   * A single (partition, boost, isInterColo) entry in the priority snapshot.
   *
   * <p>{@code boost} is the fetchSize weight currently applied to this partition: each cycle the
   * thread divides its total fetch budget in proportion to per-replica weights, so a partition
   * with {@code boost=N} gets roughly {@code N}× the per-cycle bytes of a normal replica
   * (implicit weight {@code 1}). {@code isInterColo=true} means the entry came from a
   * cross-datacenter ReplicaThread pool; {@code false} means the intra-datacenter pool.
   */
  public static class PriorityEntry {
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
}
