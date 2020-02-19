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
package com.github.ambry.protocol;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.commons.BlobId;
import com.github.ambry.store.StoreKey;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


/**
 * Contains the partition and the blob ids requested from a partition. This is used
 * by get request to specify the blob ids in a partition
 */
public class PartitionRequestInfo {

  private final PartitionId partitionId;
  private final List<BlobId> blobIds;
  private long totalIdSize;

  private static final int Blob_Id_Count_Size_InBytes = 4;

  public PartitionRequestInfo(PartitionId partitionId, List<BlobId> blobIds) {
    this.partitionId = partitionId;
    this.blobIds = blobIds;
    totalIdSize = 0;
    for (BlobId id : blobIds) {
      totalIdSize += id.sizeInBytes();
      if (!partitionId.toPathString().equals(id.getPartition().toPathString())) {
        throw new IllegalArgumentException("Not all blob IDs in GetRequest are from the same partition.");
      }
    }
  }

  public PartitionId getPartition() {
    return partitionId;
  }

  public List<? extends StoreKey> getBlobIds() {
    return blobIds;
  }

  public static PartitionRequestInfo readFrom(DataInputStream stream, ClusterMap clusterMap) throws IOException {
    int blobCount = stream.readInt();
    ArrayList<BlobId> ids = new ArrayList<BlobId>(blobCount);
    PartitionId partitionId = null;
    while (blobCount > 0) {
      BlobId id = new BlobId(stream, clusterMap);
      if (partitionId == null) {
        partitionId = id.getPartition();
      }
      ids.add(id);
      blobCount--;
    }
    return new PartitionRequestInfo(partitionId, ids);
  }

  public void writeTo(ByteBuffer byteBuffer) {
    byteBuffer.putInt(blobIds.size());
    for (BlobId blobId : blobIds) {
      byteBuffer.put(blobId.toBytes());
    }
  }

  public long sizeInBytes() {
    return Blob_Id_Count_Size_InBytes + totalIdSize;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(", ").append("PartitionId=").append(partitionId);
    sb.append("ListOfBlobIDs=").append(blobIds);
    return sb.toString();
  }
}
