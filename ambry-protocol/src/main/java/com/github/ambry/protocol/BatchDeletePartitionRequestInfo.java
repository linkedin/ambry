/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.commons.BlobId;
import com.github.ambry.store.StoreKey;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * Represents a partition and a list of blob ids that need to be deleted from that partition.
 */
public class BatchDeletePartitionRequestInfo {
  private static final int BLOB_ID_LIST_SIZE_IN_BYTES = Integer.BYTES;
  private final PartitionId partitionId;
  private final List<BlobId> blobIds;
  private long totalBlobIdsSizeInBytes;

  public BatchDeletePartitionRequestInfo(PartitionId partitionId, List<BlobId> blobIds) {
    this.partitionId = partitionId;
    this.blobIds = blobIds;
    totalBlobIdsSizeInBytes = 0;
    for (BlobId id : blobIds) {
      totalBlobIdsSizeInBytes += id.sizeInBytes();
      if (!partitionId.toPathString().equals(id.getPartition().toPathString())) {
        throw new IllegalArgumentException("Not all blob IDs in S3BatchDeleteRequest are from the same partition.");
      }
    }
  }

  /**
   * @return the {@link PartitionId} of the partition from which blobs need to be deleted.
   */
  public PartitionId getPartition() {
    return partitionId;
  }

  /**
   * @return the list of blob ids that need to be deleted from the partition.
   */
  public List<? extends StoreKey> getBlobIds() {
    return blobIds;
  }

  /**
   * Reads from a stream and constructs a {@link BatchDeletePartitionRequestInfo}.
   * @param stream the {@link DataInputStream} to read from
   * @param clusterMap the {@link ClusterMap} to use
   * @return the constructed {@link BatchDeletePartitionRequestInfo}
   * @throws IOException
   */
  public static BatchDeletePartitionRequestInfo readFrom(DataInputStream stream, ClusterMap clusterMap)
      throws IOException {
    int blobCount = stream.readInt();
    List<BlobId> blobIds = new ArrayList<>(blobCount);
    PartitionId partitionId = null;
    for (int i = 0; i < blobCount; i++) {
      BlobId blobId = new BlobId(stream, clusterMap);
      if (partitionId == null) {
        partitionId = blobId.getPartition();
      }
      blobIds.add(blobId);
    }
    return new BatchDeletePartitionRequestInfo(partitionId, blobIds);
  }

  /**
   * Writes the constituents of {@link BatchDeletePartitionRequestInfo} to the given {@link ByteBuf}.
   * @param byteBuf the {@link ByteBuf} to write to.
   */
  public void writeTo(ByteBuf byteBuf) {
    byteBuf.writeInt(blobIds.size());
    for (BlobId blobId : blobIds) {
      byteBuf.writeBytes(blobId.toBytes());
    }
  }

  /**
   * Returns the size of the serialized form of this {@link BatchDeletePartitionRequestInfo} in bytes.
   * @return the size of the serialized form of this {@link BatchDeletePartitionRequestInfo} in bytes.
   */
  public long sizeInBytes() {
    return BLOB_ID_LIST_SIZE_IN_BYTES + totalBlobIdsSizeInBytes;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("PartitionId=").append(partitionId);
    sb.append(", ").append("BlobIds=[").append(blobIds);
    sb.append("]");
    return sb.toString();
  }
}
