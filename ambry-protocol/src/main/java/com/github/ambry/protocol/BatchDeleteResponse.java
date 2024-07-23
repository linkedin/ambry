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
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * Response of batch delete request
 */
public class BatchDeleteResponse extends Response {
  private static final short Batch_Delete_Response_Version_V1 = 1;
  private static int PARTITION_RESPONSE_INFO_LIST_SIZE_IN_BYTES = Integer.BYTES;
  private final List<BatchDeletePartitionResponseInfo> partitionResponseInfoList;
  private int batchDeleteResponseSizeInBytes = 0;

  public BatchDeleteResponse(int correlationId, String clientId,
      List<BatchDeletePartitionResponseInfo> partitionResponseInfoList, ServerErrorCode error) {
    this(Batch_Delete_Response_Version_V1, correlationId, clientId, partitionResponseInfoList, error);
  }

  public BatchDeleteResponse(short versionId, int correlationId, String clientId,
      List<BatchDeletePartitionResponseInfo> partitionResponseInfoList, ServerErrorCode error) {
    super(RequestOrResponseType.BatchDeleteResponse, versionId, correlationId, clientId, error);
    this.partitionResponseInfoList = partitionResponseInfoList;
    for (BatchDeletePartitionResponseInfo partitionResponseInfo : partitionResponseInfoList) {
      batchDeleteResponseSizeInBytes += partitionResponseInfo.sizeInBytes();
    }
  }

  /**
   * Reads from a stream and constructs a {@link BatchDeleteResponse}.
   * @param stream the {@link DataInputStream} to read from
   * @param clusterMap the {@link ClusterMap} to use
   * @return the constructed {@link BatchDeleteResponse}
   * @throws IOException
   */
  public static BatchDeleteResponse readFrom(DataInputStream stream, ClusterMap clusterMap) throws IOException {
    RequestOrResponseType type = RequestOrResponseType.values()[stream.readShort()];
    if (type != RequestOrResponseType.BatchDeleteResponse) {
      throw new IllegalArgumentException("The type of request response is not compatible");
    }
    Short versionId = stream.readShort();
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    ServerErrorCode error = ServerErrorCode.values()[stream.readShort()];

    int partitionResponseInfoCount = stream.readInt();
    ArrayList<BatchDeletePartitionResponseInfo> partitionResponseInfoList = new ArrayList<>(partitionResponseInfoCount);
    for (int i = 0; i < partitionResponseInfoCount; i++) {
      BatchDeletePartitionResponseInfo partitionResponseInfo =
          BatchDeletePartitionResponseInfo.readFrom(stream, clusterMap);
      partitionResponseInfoList.add(partitionResponseInfo);
    }
    return new BatchDeleteResponse(versionId, correlationId, clientId, partitionResponseInfoList, error);
  }

  @Override
  protected void prepareBuffer() {
    super.prepareBuffer();
    if (partitionResponseInfoList != null) {
      bufferToSend.writeInt(partitionResponseInfoList.size());
      for (BatchDeletePartitionResponseInfo partitionResponseInfo : partitionResponseInfoList) {
        partitionResponseInfo.writeTo(bufferToSend);
      }
    } else {
      bufferToSend.writeInt(0);
    }
  }

  @Override
  public long sizeInBytes() {
    return super.sizeInBytes() + PARTITION_RESPONSE_INFO_LIST_SIZE_IN_BYTES + batchDeleteResponseSizeInBytes;
  }

  /**
   * Gets the delete partition response info list
   * @return the partition response info list
   */
  public List<BatchDeletePartitionResponseInfo> getPartitionResponseInfoList() {
    return partitionResponseInfoList;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("BatchDeleteResponse[");
    sb.append("ServerErrorCode=").append(getError());
    sb.append(", PartitionResponseInfoList=").append(partitionResponseInfoList);
    sb.append("]");
    return sb.toString();
  }
}
