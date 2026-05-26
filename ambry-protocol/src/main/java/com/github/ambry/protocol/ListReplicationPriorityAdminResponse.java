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
import com.github.ambry.replication.PriorityEntry;
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
  // Wire-level sanity bound; the operator-facing cap is enforced at the handler.
  private static final int MAX_ENTRIES_ON_WIRE = 10000;

  private final List<PriorityEntry> entries;
  private final byte[][] partitionIdBytes;
  private final long sizeInBytes;

  public static ListReplicationPriorityAdminResponse readFrom(DataInputStream stream, ClusterMap clusterMap)
      throws IOException {
    AdminResponse adminResponse = AdminResponse.readFrom(stream);
    Short versionId = stream.readShort();
    if (!versionId.equals(VERSION_V1)) {
      throw new IllegalStateException("Unrecognized version for ListReplicationPriorityAdminResponse: " + versionId);
    }
    int numEntries = stream.readInt();
    if (numEntries < 0 || numEntries > MAX_ENTRIES_ON_WIRE) {
      throw new IOException("ListReplicationPriorityAdminResponse numEntries out of range: " + numEntries);
    }
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
    this.partitionIdBytes = new byte[this.entries.size()][];
    for (int i = 0; i < this.entries.size(); i++) {
      this.partitionIdBytes[i] = this.entries.get(i).getPartitionId().getBytes();
    }
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
    for (int i = 0; i < entries.size(); i++) {
      PriorityEntry entry = entries.get(i);
      bufferToSend.writeBytes(partitionIdBytes[i]);
      bufferToSend.writeInt(entry.getBoost());
      bufferToSend.writeByte(entry.isInterColo() ? (byte) 1 : (byte) 0);
    }
  }

  private long computeSizeInBytes() {
    // parent + version + num-entries
    long size = super.sizeInBytes() + Short.BYTES + Integer.BYTES;
    for (int i = 0; i < entries.size(); i++) {
      // partition id + boost + isInterColo flag
      size += partitionIdBytes[i].length + Integer.BYTES + Byte.BYTES;
    }
    return size;
  }
}
