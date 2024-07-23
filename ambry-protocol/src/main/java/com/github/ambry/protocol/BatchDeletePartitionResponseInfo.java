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
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class BatchDeletePartitionResponseInfo {
  private final PartitionId partitionId;
  private final List<BlobDeleteStatus> blobsDeleteStatus;

  public BatchDeletePartitionResponseInfo(PartitionId partitionId, List<BlobDeleteStatus> deleteStatuses) {
    this.partitionId = partitionId;
    this.blobsDeleteStatus = deleteStatuses;
  }

  public PartitionId getPartition() {
    return partitionId;
  }

  /** Returns the delete status for the blobs.
   * @return the list of {@link BlobDeleteStatus}
   */
  public List<BlobDeleteStatus> getBlobsDeleteStatus() {
    return blobsDeleteStatus;
  }

  /**
   * Reads from a stream and constructs a {@link BatchDeletePartitionResponseInfo}.
   * @param stream the {@link DataInputStream} to read from
   * @param clusterMap the {@link ClusterMap} to use
   * @return the constructed {@link BatchDeletePartitionResponseInfo}
   * @throws IOException
   */
  public static BatchDeletePartitionResponseInfo readFrom(DataInputStream stream, ClusterMap clusterMap)
      throws IOException {
    int messageCount = stream.readInt();
    PartitionId partitionId = clusterMap.getPartitionIdFromStream(stream);
    ArrayList<BlobDeleteStatus> blobDeleteStatuses = new ArrayList<>(messageCount);
    for (int i = 0; i < messageCount; i++) {
      BlobDeleteStatus deleteStatus = BlobDeleteStatus.readFrom(stream, clusterMap);
      blobDeleteStatuses.add(deleteStatus);
    }
    return new BatchDeletePartitionResponseInfo(partitionId, blobDeleteStatuses);
  }

  /**
   * Writes the constituents of {@link BatchDeletePartitionResponseInfo} to the given {@link ByteBuf}.
   * @param byteBuf the {@link ByteBuf} to write to.
   */
  public void writeTo(ByteBuf byteBuf) {
    byteBuf.writeInt(blobsDeleteStatus.size());
    byteBuf.writeBytes(partitionId.getBytes());
    for (int i = 0; i < blobsDeleteStatus.size(); i++) {
      blobsDeleteStatus.get(i).writeTo(byteBuf);
    }
  }

  /**
   * Returns the size of the serialized form of this {@link BatchDeletePartitionResponseInfo} in bytes.
   * @return the size of the serialized form of this {@link BatchDeletePartitionResponseInfo} in bytes.
   */
  public long sizeInBytes() {
    long size = 0;
    size += Integer.BYTES;
    size += partitionId.getBytes().length;
    for (BlobDeleteStatus status : blobsDeleteStatus) {
      size += status.sizeInBytes();
    }
    return size;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("BatchDeletePartitionResponseInfo[");
    sb.append("PartitionId=").append(partitionId);
    sb.append(" BlobsDeleteStatus=").append(blobsDeleteStatus);
    sb.append("]");
    return sb.toString();
  }
}
