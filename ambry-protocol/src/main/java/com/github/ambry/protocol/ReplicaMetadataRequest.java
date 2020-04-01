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
import com.github.ambry.replication.FindTokenHelper;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;


/**
 * Replica metadata request to get new entries for replication
 */
public class ReplicaMetadataRequest extends RequestOrResponse {
  private List<ReplicaMetadataRequestInfo> replicaMetadataRequestInfoList;
  private long maxTotalSizeOfEntriesInBytes;
  private long replicaMetadataRequestInfoListSizeInBytes;

  private static final int Max_Entries_Size_In_Bytes = 8;
  private static final int Replica_Metadata_Request_Info_List_Size_In_Bytes = 4;
  public static final short Replica_Metadata_Request_Version_V1 = 1;
  public static final short Replica_Metadata_Request_Version_V2 = 2;

  public ReplicaMetadataRequest(int correlationId, String clientId,
      List<ReplicaMetadataRequestInfo> replicaMetadataRequestInfoList, long maxTotalSizeOfEntriesInBytes,
      short version) {
    super(RequestOrResponseType.ReplicaMetadataRequest, version, correlationId, clientId);
    if (replicaMetadataRequestInfoList == null) {
      throw new IllegalArgumentException("replicaMetadataRequestInfoList cannot be null");
    }
    validateVersion(version);
    this.replicaMetadataRequestInfoList = replicaMetadataRequestInfoList;
    this.maxTotalSizeOfEntriesInBytes = maxTotalSizeOfEntriesInBytes;
    this.replicaMetadataRequestInfoListSizeInBytes = 0;
    for (ReplicaMetadataRequestInfo replicaMetadataRequestInfo : replicaMetadataRequestInfoList) {
      this.replicaMetadataRequestInfoListSizeInBytes += replicaMetadataRequestInfo.sizeInBytes();
    }
  }

  public static ReplicaMetadataRequest readFrom(DataInputStream stream, ClusterMap clusterMap,
      FindTokenHelper findTokenHelper) throws IOException {
    Short versionId = stream.readShort();
    validateVersion(versionId);
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    int replicaMetadataRequestInfoListCount = stream.readInt();
    ArrayList<ReplicaMetadataRequestInfo> replicaMetadataRequestInfoList =
        new ArrayList<ReplicaMetadataRequestInfo>(replicaMetadataRequestInfoListCount);
    for (int i = 0; i < replicaMetadataRequestInfoListCount; i++) {
      ReplicaMetadataRequestInfo replicaMetadataRequestInfo =
          ReplicaMetadataRequestInfo.readFrom(stream, clusterMap, findTokenHelper, versionId);
      replicaMetadataRequestInfoList.add(replicaMetadataRequestInfo);
    }
    long maxTotalSizeOfEntries = stream.readLong();
    // ignore version for now
    return new ReplicaMetadataRequest(correlationId, clientId, replicaMetadataRequestInfoList, maxTotalSizeOfEntries,
        versionId);
  }

  public List<ReplicaMetadataRequestInfo> getReplicaMetadataRequestInfoList() {
    return replicaMetadataRequestInfoList;
  }

  public long getMaxTotalSizeOfEntriesInBytes() {
    return maxTotalSizeOfEntriesInBytes;
  }

  @Override
  public long writeTo(WritableByteChannel channel) throws IOException {
    if (bufferToSend == null) {
      bufferToSend = ByteBuffer.allocate((int) sizeInBytes());
      writeHeader();
      bufferToSend.putInt(replicaMetadataRequestInfoList.size());
      for (ReplicaMetadataRequestInfo replicaMetadataRequestInfo : replicaMetadataRequestInfoList) {
        replicaMetadataRequestInfo.writeTo(bufferToSend);
      }
      bufferToSend.putLong(maxTotalSizeOfEntriesInBytes);
      bufferToSend.flip();
    }
    return bufferToSend.remaining() > 0 ? channel.write(bufferToSend) : 0;
  }

  @Override
  public boolean isSendComplete() {
    return bufferToSend != null && bufferToSend.remaining() == 0;
  }

  @Override
  public long sizeInBytes() {
    return super.sizeInBytes() + Replica_Metadata_Request_Info_List_Size_In_Bytes
        + replicaMetadataRequestInfoListSizeInBytes + Max_Entries_Size_In_Bytes;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ReplicaMetadataRequest[");
    for (ReplicaMetadataRequestInfo replicaMetadataRequestInfo : replicaMetadataRequestInfoList) {
      sb.append(replicaMetadataRequestInfo.toString());
    }
    sb.append(", ").append("maxTotalSizeOfEntriesInBytes=").append(maxTotalSizeOfEntriesInBytes);
    sb.append(", ").append("ClientId=").append(clientId);
    sb.append(", ").append("CorrelationId=").append(correlationId);
    sb.append("]");
    return sb.toString();
  }

  static void validateVersion(short version) {
    if (version < Replica_Metadata_Request_Version_V1 || version > Replica_Metadata_Request_Version_V2) {
      throw new IllegalArgumentException("Invalid replicametadata request version: " + version);
    }
  }
}
