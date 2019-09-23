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
import com.github.ambry.clustermap.ReplicaType;
import com.github.ambry.replication.FindToken;
import com.github.ambry.replication.FindTokenFactory;
import com.github.ambry.replication.FindTokenHelper;
import com.github.ambry.server.ServerErrorCode;
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
  private final MessageInfoAndMetadataListSerde messageInfoAndMetadataListSerde;
  private final int messageInfoListSize;
  private final long remoteReplicaLagInBytes;
  private final PartitionId partitionId;
  private final ReplicaType replicaType;
  private final ServerErrorCode errorCode;
  private final short responseVersion;

  private long totalSizeOfAllMessages = 0;

  private static final int Error_Size_InBytes = 2;
  private static final int Remote_Replica_Lag_Size_In_Bytes = 8;
  private static final int ReplicaType_Size_In_Bytes = Short.BYTES;

  public ReplicaMetadataResponseInfo(PartitionId partitionId, ReplicaType replicaType, FindToken findToken,
      List<MessageInfo> messageInfoList, long remoteReplicaLagInBytes, short replicaMetadataResponseVersion) {
    if (partitionId == null || findToken == null || messageInfoList == null) {
      throw new IllegalArgumentException(
          "Invalid partition or token or message info list for ReplicaMetadataResponseInfo");
    }
    this.partitionId = partitionId;
    this.replicaType = replicaType;
    this.remoteReplicaLagInBytes = remoteReplicaLagInBytes;
    messageInfoAndMetadataListSerde = new MessageInfoAndMetadataListSerde(messageInfoList,
        getMessageInfoAndMetadataListSerDeVersion(replicaMetadataResponseVersion));
    messageInfoListSize = messageInfoAndMetadataListSerde.getMessageInfoAndMetadataListSize();
    this.token = findToken;
    this.errorCode = ServerErrorCode.No_Error;
    messageInfoList.forEach(info -> totalSizeOfAllMessages += info.getSize());
    responseVersion = replicaMetadataResponseVersion;
  }

  public ReplicaMetadataResponseInfo(PartitionId partitionId, ReplicaType replicaType, ServerErrorCode errorCode,
      short replicaMetadataResponseVersion) {
    if (partitionId == null) {
      throw new IllegalArgumentException("Invalid partition for ReplicaMetadataResponseInfo");
    }
    this.partitionId = partitionId;
    this.replicaType = replicaType;
    this.errorCode = errorCode;
    this.token = null;
    this.messageInfoAndMetadataListSerde = null;
    this.messageInfoListSize = 0;
    this.remoteReplicaLagInBytes = 0;
    this.responseVersion = replicaMetadataResponseVersion;
  }

  public PartitionId getPartitionId() {
    return partitionId;
  }

  public List<MessageInfo> getMessageInfoList() {
    return messageInfoAndMetadataListSerde.getMessageInfoList();
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

  public static ReplicaMetadataResponseInfo readFrom(DataInputStream stream, FindTokenHelper helper,
      ClusterMap clusterMap, short replicaMetadataResponseVersion) throws IOException {
    PartitionId partitionId = clusterMap.getPartitionIdFromStream(stream);
    ReplicaType replicaType;
    if (replicaMetadataResponseVersion == ReplicaMetadataResponse.REPLICA_METADATA_RESPONSE_VERSION_V_6) {
      replicaType = ReplicaType.values()[stream.readShort()];
    } else {
      //before REPLICA_METADATA_RESPONSE_VERSION_V_6 there were only disk based replicas
      replicaType = ReplicaType.DISK_BACKED;
    }
    ServerErrorCode error = ServerErrorCode.values()[stream.readShort()];
    if (error != ServerErrorCode.No_Error) {
      return new ReplicaMetadataResponseInfo(partitionId, replicaType, error, replicaMetadataResponseVersion);
    } else {
      FindTokenFactory findTokenFactory = helper.getFindTokenFactoryFromReplicaType(replicaType);
      FindToken token = findTokenFactory.getFindToken(stream);
      MessageInfoAndMetadataListSerde messageInfoAndMetadataList =
          MessageInfoAndMetadataListSerde.deserializeMessageInfoAndMetadataList(stream, clusterMap,
              getMessageInfoAndMetadataListSerDeVersion(replicaMetadataResponseVersion));
      long remoteReplicaLag = stream.readLong();
      return new ReplicaMetadataResponseInfo(partitionId, replicaType, token,
          messageInfoAndMetadataList.getMessageInfoList(), remoteReplicaLag, replicaMetadataResponseVersion);
    }
  }

  public void writeTo(ByteBuffer buffer) {
    buffer.put(partitionId.getBytes());
    if (responseVersion == ReplicaMetadataResponse.REPLICA_METADATA_RESPONSE_VERSION_V_6) {
      buffer.putShort((short) replicaType.ordinal());
    }
    buffer.putShort((short) errorCode.ordinal());
    if (errorCode == ServerErrorCode.No_Error) {
      buffer.put(token.toBytes());
      messageInfoAndMetadataListSerde.serializeMessageInfoAndMetadataList(buffer);
      buffer.putLong(remoteReplicaLagInBytes);
    }
  }

  public long sizeInBytes() {
    long size = (token == null ? 0 : (token.toBytes().length + Remote_Replica_Lag_Size_In_Bytes + messageInfoListSize))
        + +partitionId.getBytes().length + Error_Size_InBytes;
    if (responseVersion == ReplicaMetadataResponse.REPLICA_METADATA_RESPONSE_VERSION_V_6) {
      size += ReplicaType_Size_In_Bytes;
    }
    return size;
  }

  /**
   * @return the cumulative size of all the messages represented by this response. 0 if the response signifies an error
   */
  public long getTotalSizeOfAllMessages() {
    return totalSizeOfAllMessages;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(partitionId);
    sb.append(replicaType.name());
    sb.append(" ServerErrorCode=").append(errorCode);
    if (errorCode == ServerErrorCode.No_Error) {
      List<MessageInfo> messageInfos = messageInfoAndMetadataListSerde.getMessageInfoList();
      sb.append(" Token=").append(token);
      sb.append(" MessagesTotalSize=").append(totalSizeOfAllMessages);
      if (messageInfos != null) {
        int size = messageInfos.size();
        sb.append(" MessageInfoListSize=").append(size);
        if (size > 0) {
          sb.append(" MessageInfoListFirstId=").append(messageInfos.get(0).getStoreKey());
          sb.append(" MessageInfoListLastId=").append(messageInfos.get(size - 1).getStoreKey());
        }
      }
      sb.append(" RemoteReplicaLagInBytes=").append(remoteReplicaLagInBytes);
    }
    return sb.toString();
  }

  /**
   * Return the MessageInfoAndMetadataList SerDe version to use for the given {@link ReplicaMetadataResponse} version
   * @param replicaMetadataResponseVersion the {@link ReplicaMetadataResponse} version
   * @return the MessageInfoAndMetadataList SerDe version to use for the given {@link ReplicaMetadataResponse} version
   */
  private static short getMessageInfoAndMetadataListSerDeVersion(short replicaMetadataResponseVersion) {
    switch (replicaMetadataResponseVersion) {
      case ReplicaMetadataResponse.REPLICA_METADATA_RESPONSE_VERSION_V_1:
        return MessageInfoAndMetadataListSerde.VERSION_1;
      case ReplicaMetadataResponse.REPLICA_METADATA_RESPONSE_VERSION_V_2:
        return MessageInfoAndMetadataListSerde.VERSION_2;
      case ReplicaMetadataResponse.REPLICA_METADATA_RESPONSE_VERSION_V_3:
        return MessageInfoAndMetadataListSerde.VERSION_3;
      case ReplicaMetadataResponse.REPLICA_METADATA_RESPONSE_VERSION_V_4:
        return MessageInfoAndMetadataListSerde.VERSION_4;
      case ReplicaMetadataResponse.REPLICA_METADATA_RESPONSE_VERSION_V_5:
      case ReplicaMetadataResponse.REPLICA_METADATA_RESPONSE_VERSION_V_6:
        return MessageInfoAndMetadataListSerde.DETERMINE_VERSION;
      default:
        throw new IllegalArgumentException(
            "Unknown ReplicaMetadataResponse version encountered: " + replicaMetadataResponseVersion);
    }
  }
}
