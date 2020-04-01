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
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;


/**
 * GetRequest to fetch data
 */
public class GetRequest extends RequestOrResponse {

  private MessageFormatFlags flags;
  private GetOption getOption;
  private List<PartitionRequestInfo> partitionRequestInfoList;
  private int sizeSent;
  private int totalPartitionRequestInfoListSize;

  private static final int MessageFormat_Size_In_Bytes = 2;
  private static final int GetOption_Size_In_Bytes = 2;
  private static final int Partition_Request_Info_List_Size = 4;
  private static final short Get_Request_Version_V2 = 2;
  public static final String Replication_Client_Id_Prefix = "replication-fetch-";

  public GetRequest(int correlationId, String clientId, MessageFormatFlags flags,
      List<PartitionRequestInfo> partitionRequestInfoList, GetOption getOption) {
    super(RequestOrResponseType.GetRequest, Get_Request_Version_V2, correlationId, clientId);

    this.flags = flags;
    this.getOption = getOption;
    if (partitionRequestInfoList == null) {
      throw new IllegalArgumentException("No partition info specified in GetRequest");
    }
    this.partitionRequestInfoList = partitionRequestInfoList;
    for (PartitionRequestInfo partitionRequestInfo : partitionRequestInfoList) {
      totalPartitionRequestInfoListSize += partitionRequestInfo.sizeInBytes();
    }
    this.sizeSent = 0;
  }

  public MessageFormatFlags getMessageFormatFlag() {
    return flags;
  }

  public List<PartitionRequestInfo> getPartitionInfoList() {
    return partitionRequestInfoList;
  }

  public GetOption getGetOption() {
    return getOption;
  }

  public static GetRequest readFrom(DataInputStream stream, ClusterMap clusterMap) throws IOException {
    RequestOrResponseType type = RequestOrResponseType.GetRequest;
    Short versionId = stream.readShort();
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    MessageFormatFlags messageType = MessageFormatFlags.values()[stream.readShort()];
    int totalNumberOfPartitionInfo = stream.readInt();
    ArrayList<PartitionRequestInfo> partitionRequestInfoList =
        new ArrayList<PartitionRequestInfo>(totalNumberOfPartitionInfo);
    for (int i = 0; i < totalNumberOfPartitionInfo; i++) {
      PartitionRequestInfo partitionRequestInfo = PartitionRequestInfo.readFrom(stream, clusterMap);
      partitionRequestInfoList.add(partitionRequestInfo);
    }
    GetOption getOption = GetOption.None;
    if (versionId == Get_Request_Version_V2) {
      getOption = GetOption.values()[stream.readShort()];
    }
    // ignore version for now
    return new GetRequest(correlationId, clientId, messageType, partitionRequestInfoList, getOption);
  }

  @Override
  public long writeTo(WritableByteChannel channel) throws IOException {
    long written = 0;
    if (bufferToSend == null) {
      bufferToSend = ByteBuffer.allocate((int) sizeInBytes());
      writeHeader();
      bufferToSend.putShort((short) flags.ordinal());
      bufferToSend.putInt(partitionRequestInfoList.size());
      for (PartitionRequestInfo partitionRequestInfo : partitionRequestInfoList) {
        partitionRequestInfo.writeTo(bufferToSend);
      }
      bufferToSend.putShort((short) getOption.ordinal());
      bufferToSend.flip();
    }
    if (bufferToSend.remaining() > 0) {
      written = channel.write(bufferToSend);
      sizeSent += written;
    }
    return written;
  }

  @Override
  public boolean isSendComplete() {
    return sizeSent == sizeInBytes();
  }

  @Override
  public long sizeInBytes() {
    // header + message format size + partition request info size + total partition request info list size
    return super.sizeInBytes() + MessageFormat_Size_In_Bytes + Partition_Request_Info_List_Size
        + totalPartitionRequestInfoListSize + GetOption_Size_In_Bytes;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("GetRequest[");
    for (PartitionRequestInfo partitionRequestInfo : partitionRequestInfoList) {
      sb.append(partitionRequestInfo.toString());
    }
    sb.append(", ").append("ClientId=").append(clientId);
    sb.append(", ").append("CorrelationId=").append(correlationId);
    sb.append(", ").append("MessageFormatFlags=").append(flags);
    sb.append(", ").append("GetOption=").append(getOption);
    sb.append("]");
    return sb.toString();
  }
}
