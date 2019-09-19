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
import com.github.ambry.commons.BlobId;
import com.github.ambry.messageformat.MessageFormatFlags;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * GetRequest to fetch data
 */
public class GetRequest extends RequestOrResponse {

  private MessageFormatFlags flags;
  private GetOption getOption;
  private List<PartitionRequestInfo> partitionRequestInfoList;
  private Map<BlobId, GetBlobStoreOption> getBlobStoreOptions;
  private int sizeSent;
  private int totalPartitionRequestInfoListSize;
  private int totalGetBlobStoreOptionSize = 0;

  private static final int MessageFormat_Size_In_Bytes = 2;
  private static final int GetOption_Size_In_Bytes = 2;
  private static final int Partition_Request_Info_List_Size = 4;
  private static final int GetBlobStoreOption_Size_In_Bytes = 4;
  private static final short Get_Request_Version_V2 = 2;
  private static final short Get_Request_Version_V3 = 3;
  public static final String Replication_Client_Id_Prefix = "replication-fetch-";
  public static final String Cloud_Replica_Keyword = "vcr";

  public GetRequest(int correlationId, String clientId, MessageFormatFlags flags,
      List<PartitionRequestInfo> partitionRequestInfoList, GetOption getOption) {
    this(correlationId, clientId, flags, partitionRequestInfoList, getOption, Get_Request_Version_V2);
  }

  public GetRequest(int correlationId, String clientId, MessageFormatFlags flags,
      List<PartitionRequestInfo> partitionRequestInfoList, GetOption getOption,
      Map<BlobId, GetBlobStoreOption> getBlobStoreOptions) {
    this(correlationId, clientId, flags, partitionRequestInfoList, getOption, Get_Request_Version_V3);;
    this.getBlobStoreOptions = getBlobStoreOptions;
    if (getBlobStoreOptions != null) {
      for (Map.Entry<BlobId, GetBlobStoreOption> entry : getBlobStoreOptions.entrySet()) {
        this.totalGetBlobStoreOptionSize += entry.getKey().sizeInBytes();
        this.totalGetBlobStoreOptionSize += entry.getValue().sizeInBytes();
      }
    }
  }

  private GetRequest(int correlationId, String clientId, MessageFormatFlags flags,
      List<PartitionRequestInfo> partitionRequestInfoList, GetOption getOption, short version) {
    super(RequestOrResponseType.GetRequest, version, correlationId, clientId);

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

  public Map<BlobId, GetBlobStoreOption> getGetBlobStoreOptions() {
    return getBlobStoreOptions;
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
    System.out.println("Version of getrequest is " + versionId);
    Map<BlobId, GetBlobStoreOption> getBlobStoreOptionMap = null;
    if (versionId >= Get_Request_Version_V3) {
      getBlobStoreOptionMap = new HashMap<>();
      int mapSize = stream.readInt();
      for (int i = 0; i < mapSize; i ++) {
        BlobId blobId = new BlobId(stream, clusterMap);
        GetBlobStoreOption option = new GetBlobStoreOption(stream);
        getBlobStoreOptionMap.put(blobId, option);
      }
    }
    // ignore version for now
    return new GetRequest(correlationId, clientId, messageType, partitionRequestInfoList, getOption, getBlobStoreOptionMap);
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
        if (getBlobStoreOptions == null || getBlobStoreOptions.isEmpty()) {
          bufferToSend.putInt(0);
        } else {
          System.out.println("Putting get blob store options to buffer, size is " + getBlobStoreOptions.size());
          bufferToSend.putInt(getBlobStoreOptions.size());
          for (Map.Entry<BlobId, GetBlobStoreOption> entry: getBlobStoreOptions.entrySet()) {
            bufferToSend.put(entry.getKey().toBytes());
            entry.getValue().writeTo(bufferToSend);
          }
        }
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
    // header + message format size + partition request info size + total partition request info list size
    long size = super.sizeInBytes() + MessageFormat_Size_In_Bytes + Partition_Request_Info_List_Size
        + totalPartitionRequestInfoListSize + GetOption_Size_In_Bytes;
    if (versionId == Get_Request_Version_V3) {
      size += GetBlobStoreOption_Size_In_Bytes + totalGetBlobStoreOptionSize;
    }
    return size;
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
