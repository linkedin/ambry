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
  private final int messageInfoListSize;
  private final MessageInfoListSerde messageInfoListSerDe;
  private ServerErrorCode errorCode;

  private static final int Error_Size_InBytes = 2;

  public PartitionResponseInfo(PartitionId partitionId, List<MessageInfo> messageInfoList) {
    this.messageInfoListSerDe = new MessageInfoListSerde(messageInfoList);
    this.messageInfoListSize = messageInfoListSerDe.getMessageInfoListSize();
    this.partitionId = partitionId;
    this.errorCode = ServerErrorCode.No_Error;
  }

  public PartitionResponseInfo(PartitionId partitionId, ServerErrorCode errorCode) {
    List<MessageInfo> messageInfoList = new ArrayList<MessageInfo>();
    this.messageInfoListSerDe = new MessageInfoListSerde(messageInfoList);
    this.messageInfoListSize = messageInfoListSerDe.getMessageInfoListSize();
    this.partitionId = partitionId;
    this.errorCode = errorCode;
  }

  public PartitionId getPartition() {
    return partitionId;
  }

  public List<MessageInfo> getMessageInfoList() {
    return messageInfoListSerDe.getMessageInfoList();
  }

  public ServerErrorCode getErrorCode() {
    return errorCode;
  }

  public static PartitionResponseInfo readFrom(DataInputStream stream, ClusterMap map) throws IOException {
    PartitionId partitionId = map.getPartitionIdFromStream(stream);
    List<MessageInfo> messageInfoList = MessageInfoListSerde.deserializeMessageInfoList(stream, map);
    ServerErrorCode error = ServerErrorCode.values()[stream.readShort()];
    if (error != ServerErrorCode.No_Error) {
      return new PartitionResponseInfo(partitionId, error);
    } else {
      return new PartitionResponseInfo(partitionId, messageInfoList);
    }
  }

  public void writeTo(ByteBuffer byteBuffer) {
    byteBuffer.put(partitionId.getBytes());
    messageInfoListSerDe.serializeMessageInfoList(byteBuffer);
    byteBuffer.putShort((short) errorCode.ordinal());
  }

  public long sizeInBytes() {
    return partitionId.getBytes().length + messageInfoListSize + Error_Size_InBytes;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("PartitionResponseInfo[");
    sb.append("PartitionId=").append(partitionId);
    sb.append(" ServerErrorCode=").append(errorCode);
    sb.append(" MessageInfoListSize=").append(messageInfoListSize);
    sb.append("]");
    return sb.toString();
  }
}
