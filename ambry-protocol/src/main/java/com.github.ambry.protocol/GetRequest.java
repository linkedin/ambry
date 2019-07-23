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
import com.github.ambry.store.Message;
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
  private final Long ifModifiedSince;
  private int sizeSent;
  private int totalPartitionRequestInfoListSize;

  private static final int MessageFormat_Size_In_Bytes = 2;
  private static final int GetOption_Size_In_Bytes = 2;
  private static final int Partition_Request_Info_List_Size = 4;
  private static final int IfModifiedSince_Size_In_Bytes = 8;
  private static final short Get_Request_Version_V2 = 2;
  private static final short Get_Request_Version_V3 = 3;
  public static final String Replication_Client_Id_Prefix = "replication-fetch-";
  public static final String Cloud_Replica_Keyword = "vcr";
  public static final Long EMPTY_IF_MODIFED_SINCE = new Long(-1);

  public GetRequest(int correlationId, String clientId, MessageFormatFlags flags,
      List<PartitionRequestInfo> partitionRequestInfoList, GetOption getOption, Long ifModifiedSince) {
    super(RequestOrResponseType.GetRequest, Get_Request_Version_V3, correlationId, clientId);

    this.flags = flags;
    this.getOption = getOption;
    this.ifModifiedSince = ifModifiedSince;
    if (partitionRequestInfoList == null) {
      throw new IllegalArgumentException("No partition info specified in GetRequest");
    }
    if (this.ifModifiedSince != EMPTY_IF_MODIFED_SINCE) {
      // Make sure partition request info list only have one partition and one blob
      if (this.partitionRequestInfoList.size() > 1) {
        throw new IllegalArgumentException("If-Modified-Since should be used with one partition and one blob");
      }
      if (this.flags != MessageFormatFlags.All) {
        throw new IllegalArgumentException("If-Modified-Since should be used with blob or all message format flag");
      }
      if (this.getOption == GetOption.Include_All || this.getOption == GetOption.Include_Deleted_Blobs) {
        throw new IllegalArgumentException("If-Modified-Since shouldn't be used with deleted blob");
      }
    }
    this.partitionRequestInfoList = partitionRequestInfoList;
    for (PartitionRequestInfo partitionRequestInfo : partitionRequestInfoList) {
      if (this.ifModifiedSince != EMPTY_IF_MODIFED_SINCE && partitionRequestInfo.getBlobIds().size() > 1) {
        throw new IllegalArgumentException("If-Modified-Since should be used with one partition and one blob");
      }
      totalPartitionRequestInfoListSize += partitionRequestInfo.sizeInBytes();
    }
    this.sizeSent = 0;
  }

  public GetRequest(int correlationId, String clientId, MessageFormatFlags flags,
      List<PartitionRequestInfo> partitionRequestInfoList, GetOption getOption) {
    super(RequestOrResponseType.GetRequest, Get_Request_Version_V2, correlationId, clientId);

    this.flags = flags;
    this.getOption = getOption;
    this.ifModifiedSince = EMPTY_IF_MODIFED_SINCE;
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

  public Long getIfModifiedSince() {
    return ifModifiedSince;
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
    if (versionId >= Get_Request_Version_V2) {
      getOption = GetOption.values()[stream.readShort()];
    }
    Long ifModifiedSince = EMPTY_IF_MODIFED_SINCE;
    if (versionId == Get_Request_Version_V3) {
      ifModifiedSince = stream.readLong();
    }
    if (versionId == Get_Request_Version_V2) {
      return new GetRequest(correlationId, clientId, messageType, partitionRequestInfoList, getOption);
    } else {
      return new GetRequest(correlationId, clientId, messageType, partitionRequestInfoList, getOption, ifModifiedSince);
    }
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
      if (versionId == Get_Request_Version_V3) {
        bufferToSend.putLong(ifModifiedSince);
      }
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
    int ifModifiedSinceSize = versionId == Get_Request_Version_V3 ? IfModifiedSince_Size_In_Bytes : 0;
    // header + message format size + partition request info size + total partition request info list
    return super.sizeInBytes() + MessageFormat_Size_In_Bytes + Partition_Request_Info_List_Size
        + totalPartitionRequestInfoListSize + GetOption_Size_In_Bytes + ifModifiedSinceSize;
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
    sb.append(", ").append("IfModifiedSince=").append(ifModifiedSince);
    sb.append("]");
    return sb.toString();
  }
}
