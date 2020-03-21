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
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.messageformat.MessageMetadata;
import com.github.ambry.store.MessageInfo;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


/**
 * Contains the partition and the message info list for a partition. This is used by
 * get response to specify the message info list in a partition
 */
public class PartitionResponseInfo {

  private final PartitionId partitionId;
  private final int messageInfoAndMetadataListSize;
  private final MessageInfoAndMetadataListSerde messageInfoAndMetadataListSerde;
  private ServerErrorCode errorCode;

  private static final int Error_Size_InBytes = 2;

  private PartitionResponseInfo(PartitionId partitionId, List<MessageInfo> messageInfoList,
      List<MessageMetadata> messageMetadataList, ServerErrorCode serverErrorCode, short getResponseVersion) {
    this.messageInfoAndMetadataListSerde = new MessageInfoAndMetadataListSerde(messageInfoList, messageMetadataList,
        getMessageInfoAndMetadataListSerDeVersion(getResponseVersion));
    this.messageInfoAndMetadataListSize = messageInfoAndMetadataListSerde.getMessageInfoAndMetadataListSize();
    this.partitionId = partitionId;
    this.errorCode = serverErrorCode;
  }

  public PartitionResponseInfo(PartitionId partitionId, List<MessageInfo> messageInfoList,
      List<MessageMetadata> messageMetadataList) {
    this(partitionId, messageInfoList, messageMetadataList, ServerErrorCode.No_Error, GetResponse.getCurrentVersion());
  }

  public PartitionResponseInfo(PartitionId partitionId, ServerErrorCode errorCode) {
    this(partitionId, new ArrayList<>(), new ArrayList<>(), errorCode, GetResponse.getCurrentVersion());
  }

  public PartitionId getPartition() {
    return partitionId;
  }

  public List<MessageInfo> getMessageInfoList() {
    return messageInfoAndMetadataListSerde.getMessageInfoList();
  }

  public List<MessageMetadata> getMessageMetadataList() {
    return messageInfoAndMetadataListSerde.getMessageMetadataList();
  }

  public ServerErrorCode getErrorCode() {
    return errorCode;
  }

  public static PartitionResponseInfo readFrom(DataInputStream stream, ClusterMap map, short getResponseVersion)
      throws IOException {
    PartitionId partitionId = map.getPartitionIdFromStream(stream);
    MessageInfoAndMetadataListSerde messageInfoAndMetadataList =
        MessageInfoAndMetadataListSerde.deserializeMessageInfoAndMetadataList(stream, map,
            getMessageInfoAndMetadataListSerDeVersion(getResponseVersion));
    ServerErrorCode error = ServerErrorCode.values()[stream.readShort()];
    if (error != ServerErrorCode.No_Error) {
      return new PartitionResponseInfo(partitionId, new ArrayList<>(), new ArrayList<>(), error, getResponseVersion);
    } else {
      return new PartitionResponseInfo(partitionId, messageInfoAndMetadataList.getMessageInfoList(),
          messageInfoAndMetadataList.getMessageMetadataList(), ServerErrorCode.No_Error, getResponseVersion);
    }
  }

  public void writeTo(ByteBuffer byteBuffer) {
    byteBuffer.put(partitionId.getBytes());
    messageInfoAndMetadataListSerde.serializeMessageInfoAndMetadataList(byteBuffer);
    byteBuffer.putShort((short) errorCode.ordinal());
  }

  public long sizeInBytes() {
    return partitionId.getBytes().length + messageInfoAndMetadataListSize + Error_Size_InBytes;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("PartitionResponseInfo[");
    sb.append("PartitionId=").append(partitionId);
    sb.append(" ServerErrorCode=").append(errorCode);
    sb.append(" MessageInfoAndMetadataListSize=").append(messageInfoAndMetadataListSize);
    sb.append("]");
    return sb.toString();
  }

  /**
   * Return the SerDe version for MessageInfoAndMetadataList to use for the given {@link GetResponse} version
   * @param getResponseVersion the GetResponse version
   * @return the MessageInfoAndMetadataList SerDe version to use for the given GetResponse version
   */
  private static short getMessageInfoAndMetadataListSerDeVersion(short getResponseVersion) {
    switch (getResponseVersion) {
      case GetResponse.GET_RESPONSE_VERSION_V_1:
        return MessageInfoAndMetadataListSerde.VERSION_1;
      case GetResponse.GET_RESPONSE_VERSION_V_2:
        return MessageInfoAndMetadataListSerde.VERSION_2;
      case GetResponse.GET_RESPONSE_VERSION_V_3:
        return MessageInfoAndMetadataListSerde.VERSION_3;
      case GetResponse.GET_RESPONSE_VERSION_V_4:
        return MessageInfoAndMetadataListSerde.VERSION_4;
      case GetResponse.GET_RESPONSE_VERSION_V_5:
        return MessageInfoAndMetadataListSerde.DETERMINE_VERSION;
      default:
        throw new IllegalArgumentException("Unknown GetResponse version encountered: " + getResponseVersion);
    }
  }
}
