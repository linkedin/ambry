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
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * Delete request to delete a list of blobs that spans multiple partitions of a data node.
 */
public class BatchDeleteRequest extends RequestOrResponse {
  private final static short BATCH_DELETE_REQUEST_VERSION_1 = 1;
  private final static short CURRENT_VERSION = BATCH_DELETE_REQUEST_VERSION_1;
  private static final int BATCH_DELETE_PARTITION_REQUEST_INFO_LIST_SIZE_IN_BYTES = Integer.BYTES;
  private static final int DELETION_TIME_FIELD_SIZE_IN_BYTES = Long.BYTES;
  private List<BatchDeletePartitionRequestInfo> batchDeletePartitionRequestInfos;
  private final long deletionTimeInMs;
  private int totalPartitionRequestInfoListSizeInBytes;

  /**
   * Constructs {@link BatchDeleteRequest} in {@link #BATCH_DELETE_REQUEST_VERSION_1}
   * @param correlationId correlationId of the delete request
   * @param clientId clientId of the delete request
   * @param batchDeletePartitionRequestInfos blobIds of the delete request
   * @param deletionTimeInMs deletion time of the blob in ms
   */
  public BatchDeleteRequest(int correlationId, String clientId,
      List<BatchDeletePartitionRequestInfo> batchDeletePartitionRequestInfos, long deletionTimeInMs) {
    this(correlationId, clientId, batchDeletePartitionRequestInfos, deletionTimeInMs, CURRENT_VERSION);
  }

  /**
   * Constructs {@link BatchDeleteRequest} in given version.
   * @param correlationId correlationId of the batch delete request
   * @param clientId clientId of the batch delete request
   * @param batchDeletePartitionRequestInfos blobIds grouped by partitions
   * @param deletionTimeInMs deletion time of the blobs in ms
   * @param version version of the {@link BatchDeleteRequest}
   */
  protected BatchDeleteRequest(int correlationId, String clientId,
      List<BatchDeletePartitionRequestInfo> batchDeletePartitionRequestInfos, long deletionTimeInMs, short version) {
    super(RequestOrResponseType.DeleteRequest, version, correlationId, clientId);
    this.batchDeletePartitionRequestInfos = batchDeletePartitionRequestInfos;
    if (batchDeletePartitionRequestInfos == null) {
      throw new IllegalArgumentException("No partition info specified in BatchDeleteRequest");
    }
    for (BatchDeletePartitionRequestInfo partitionRequestInfo : batchDeletePartitionRequestInfos) {
      totalPartitionRequestInfoListSizeInBytes += partitionRequestInfo.sizeInBytes();
    }
    this.deletionTimeInMs = deletionTimeInMs;
  }

  /**
   * Constructs {@link BatchDeleteRequest} reading from a stream {@link DataInputStream}
   * @param stream the stream to read from
   * @param clusterMap the {@link ClusterMap} to use
   * @return the constructed {@link BatchDeleteRequest}
   */
  public static BatchDeleteRequest readFrom(DataInputStream stream, ClusterMap clusterMap) throws IOException {
    Short version = stream.readShort();
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    int totalNumberOfPartitionInfo = stream.readInt();
    ArrayList<BatchDeletePartitionRequestInfo> partitionRequestInfoList = new ArrayList<>(totalNumberOfPartitionInfo);
    for (int i = 0; i < totalNumberOfPartitionInfo; i++) {
      BatchDeletePartitionRequestInfo partitionRequestInfo =
          BatchDeletePartitionRequestInfo.readFrom(stream, clusterMap);
      partitionRequestInfoList.add(partitionRequestInfo);
    }
    long deletionTimeInMs = stream.readLong();
    return new BatchDeleteRequest(correlationId, clientId, partitionRequestInfoList, deletionTimeInMs);
  }

  @Override
  public void accept(RequestVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  protected void prepareBuffer() {
    super.prepareBuffer();
    bufferToSend.writeInt(batchDeletePartitionRequestInfos.size());
    for (BatchDeletePartitionRequestInfo partitionRequestInfo : batchDeletePartitionRequestInfos) {
      partitionRequestInfo.writeTo(bufferToSend);
    }
    bufferToSend.writeLong(deletionTimeInMs);
  }

  /**
   * Gets the list of {@link BatchDeletePartitionRequestInfo} in the {@link BatchDeleteRequest}
   * @return the list of {@link BatchDeletePartitionRequestInfo}
   */
  public List<BatchDeletePartitionRequestInfo> getPartitionRequestInfoList() {
    return batchDeletePartitionRequestInfos;
  }

  /**
   * Gets the deletion time in ms
   * @return the deletion time in ms
   */
  public long getDeletionTimeInMs() {
    return deletionTimeInMs;
  }

  @Override
  public long sizeInBytes() {
    return super.sizeInBytes() + BATCH_DELETE_PARTITION_REQUEST_INFO_LIST_SIZE_IN_BYTES
        + totalPartitionRequestInfoListSizeInBytes + DELETION_TIME_FIELD_SIZE_IN_BYTES;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("BatchDeleteRequest[");
    for (BatchDeletePartitionRequestInfo partitionRequestInfo : batchDeletePartitionRequestInfos) {
      sb.append(partitionRequestInfo.toString());
    }
    sb.append(", ").append("ClientId=").append(clientId);
    sb.append(", ").append("CorrelationId=").append(correlationId);
    sb.append(", ").append("Version=").append(CURRENT_VERSION);
    sb.append("]");
    return sb.toString();
  }
}
