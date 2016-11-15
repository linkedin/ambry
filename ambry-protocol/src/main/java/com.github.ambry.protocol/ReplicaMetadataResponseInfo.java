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
import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.store.FindToken;
import com.github.ambry.store.FindTokenFactory;
import com.github.ambry.store.MessageInfo;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;


/**
 * Contains the token, messageInfoList, remoteReplicaLag for a local partition. This is used
 * by replica metadata response to specify information for a partition
 */
public class ReplicaMetadataResponseInfo {
  private final FindToken token;
  private final MessageInfoListSerde messageInfoListSerDe;
  private final int messageInfoListSize;
  private final long remoteReplicaLagInBytes;
  private final PartitionId partitionId;
  private final ServerErrorCode errorCode;

  private static final int Error_Size_InBytes = 2;
  private static final int Remote_Replica_Lag_Size_In_Bytes = 8;

  public ReplicaMetadataResponseInfo(PartitionId partitionId, FindToken findToken, List<MessageInfo> messageInfoList,
      long remoteReplicaLagInBytes) {
    if (partitionId == null || findToken == null || messageInfoList == null) {
      throw new IllegalArgumentException(
          "Invalid partition or token or message info list for ReplicaMetadataResponseInfo");
    }
    this.partitionId = partitionId;
    this.remoteReplicaLagInBytes = remoteReplicaLagInBytes;
    messageInfoListSerDe = new MessageInfoListSerde(messageInfoList);
    messageInfoListSize = messageInfoListSerDe.getMessageInfoListSize();
    this.token = findToken;
    this.errorCode = ServerErrorCode.No_Error;
  }

  public ReplicaMetadataResponseInfo(PartitionId partitionId, ServerErrorCode errorCode) {
    if (partitionId == null) {
      throw new IllegalArgumentException("Invalid partition for ReplicaMetadataResponseInfo");
    }
    this.partitionId = partitionId;
    this.errorCode = errorCode;
    this.token = null;
    this.messageInfoListSerDe = null;
    this.messageInfoListSize = 0;
    this.remoteReplicaLagInBytes = 0;
  }

  public PartitionId getPartitionId() {
    return partitionId;
  }

  public List<MessageInfo> getMessageInfoList() {
    return messageInfoListSerDe.getMessageInfoList();
  }

  public FindToken getFindToken() {
    return token;
  }

  public long getRemoteReplicaLagInBytes() {
    return remoteReplicaLagInBytes;
  }

  public ServerErrorCode getError() {
    return errorCode;
  }

  public static ReplicaMetadataResponseInfo readFrom(DataInputStream stream, FindTokenFactory factory,
      ClusterMap clusterMap) throws IOException {
    PartitionId partitionId = clusterMap.getPartitionIdFromStream(stream);
    ServerErrorCode error = ServerErrorCode.values()[stream.readShort()];
    if (error != ServerErrorCode.No_Error) {
      return new ReplicaMetadataResponseInfo(partitionId, error);
    } else {
      FindToken token = factory.getFindToken(stream);
      List<MessageInfo> messageInfoList = MessageInfoListSerde.deserializeMessageInfoList(stream, clusterMap);
      long remoteReplicaLag = stream.readLong();
      return new ReplicaMetadataResponseInfo(partitionId, token, messageInfoList, remoteReplicaLag);
    }
  }

  public void writeTo(ByteBuffer buffer) {
    buffer.put(partitionId.getBytes());
    buffer.putShort((short) errorCode.ordinal());
    if (errorCode == ServerErrorCode.No_Error) {
      buffer.put(token.toBytes());
      messageInfoListSerDe.serializeMessageInfoList(buffer);
      buffer.putLong(remoteReplicaLagInBytes);
    }
  }

  public long sizeInBytes() {
    return (token == null ? 0 : (token.toBytes().length + Remote_Replica_Lag_Size_In_Bytes + messageInfoListSize))
        + +partitionId.getBytes().length + Error_Size_InBytes;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(partitionId);
    sb.append(" ServerErrorCode=").append(errorCode);
    if (errorCode == ServerErrorCode.No_Error) {
      sb.append(" Token=").append(token);
      sb.append(" MessageInfoList=").append(messageInfoListSerDe.getMessageInfoList());
      sb.append(" RemoteReplicaLagInBytes=").append(remoteReplicaLagInBytes);
    }
    return sb.toString();
  }
}
