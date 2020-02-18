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
import com.github.ambry.network.Send;
import com.github.ambry.router.AsyncWritableChannel;
import com.github.ambry.router.Callback;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;


/**
 * Response to GetRequest to fetch data
 */
public class GetResponse extends Response {

  private Send toSend = null;
  private InputStream stream = null;
  private final List<PartitionResponseInfo> partitionResponseInfoList;
  private int partitionResponseInfoSize;

  private static int Partition_Response_Info_List_Size = 4;
  static final short GET_RESPONSE_VERSION_V_1 = 1;
  static final short GET_RESPONSE_VERSION_V_2 = 2;
  static final short GET_RESPONSE_VERSION_V_3 = 3;
  static final short GET_RESPONSE_VERSION_V_4 = 4;
  static final short GET_RESPONSE_VERSION_V_5 = 5;

  static short CURRENT_VERSION = GET_RESPONSE_VERSION_V_5;

  public GetResponse(int correlationId, String clientId, List<PartitionResponseInfo> partitionResponseInfoList,
      Send send, ServerErrorCode error) {
    super(RequestOrResponseType.GetResponse, CURRENT_VERSION, correlationId, clientId, error);
    this.partitionResponseInfoList = partitionResponseInfoList;
    this.partitionResponseInfoSize = 0;
    for (PartitionResponseInfo partitionResponseInfo : partitionResponseInfoList) {
      this.partitionResponseInfoSize += partitionResponseInfo.sizeInBytes();
    }
    this.toSend = send;
  }

  public GetResponse(int correlationId, String clientId, List<PartitionResponseInfo> partitionResponseInfoList,
      InputStream stream, ServerErrorCode error) {
    super(RequestOrResponseType.GetResponse, CURRENT_VERSION, correlationId, clientId, error);
    this.partitionResponseInfoList = partitionResponseInfoList;
    this.partitionResponseInfoSize = 0;
    for (PartitionResponseInfo partitionResponseInfo : partitionResponseInfoList) {
      this.partitionResponseInfoSize += partitionResponseInfo.sizeInBytes();
    }
    this.stream = stream;
  }

  public GetResponse(int correlationId, String clientId, ServerErrorCode error) {
    super(RequestOrResponseType.GetResponse, CURRENT_VERSION, correlationId, clientId, error);
    this.partitionResponseInfoList = null;
    this.partitionResponseInfoSize = 0;
  }

  public InputStream getInputStream() {
    return stream;
  }

  public List<PartitionResponseInfo> getPartitionResponseInfoList() {
    return partitionResponseInfoList;
  }

  public static GetResponse readFrom(DataInputStream stream, ClusterMap map) throws IOException {
    short typeval = stream.readShort();
    RequestOrResponseType type = RequestOrResponseType.values()[typeval];
    if (type != RequestOrResponseType.GetResponse) {
      throw new IllegalArgumentException("The type of request response is not compatible");
    }
    Short versionId = stream.readShort();
    // ignore version for now
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    ServerErrorCode error = ServerErrorCode.values()[stream.readShort()];

    if (error != ServerErrorCode.No_Error) {
      return new GetResponse(correlationId, clientId, error);
    } else {
      int partitionResponseInfoCount = stream.readInt();
      ArrayList<PartitionResponseInfo> partitionResponseInfoList =
          new ArrayList<PartitionResponseInfo>(partitionResponseInfoCount);
      for (int i = 0; i < partitionResponseInfoCount; i++) {
        PartitionResponseInfo partitionResponseInfo = PartitionResponseInfo.readFrom(stream, map, versionId);
        partitionResponseInfoList.add(partitionResponseInfo);
      }
      return new GetResponse(correlationId, clientId, partitionResponseInfoList, stream, error);
    }
  }

  /**
   * A private method shared by {@link GetResponse#writeTo(WritableByteChannel)} and
   * {@link GetResponse#writeTo(AsyncWritableChannel, Callback)}.
   * This method allocate bufferToSend and write metadata to it if bufferToSend is null.
   */
  private void prepareBufferToSend() {
    if (bufferToSend == null) {
      bufferToSend = ByteBuffer.allocate(
          (int) super.sizeInBytes() + (Partition_Response_Info_List_Size + partitionResponseInfoSize));
      writeHeader();
      if (partitionResponseInfoList != null) {
        bufferToSend.putInt(partitionResponseInfoList.size());
        for (PartitionResponseInfo partitionResponseInfo : partitionResponseInfoList) {
          partitionResponseInfo.writeTo(bufferToSend);
        }
      }
      bufferToSend.flip();
    }
  }

  @Override
  public long writeTo(WritableByteChannel channel) throws IOException {
    prepareBufferToSend();
    long written = 0;
    if (bufferToSend.remaining() > 0) {
      written = channel.write(bufferToSend);
    }
    if (bufferToSend.remaining() == 0 && toSend != null && !toSend.isSendComplete()) {
      written += toSend.writeTo(channel);
    }
    return written;
  }

  @Override
  public void writeTo(AsyncWritableChannel channel, Callback<Long> callback) {
    prepareBufferToSend();
    channel.write(bufferToSend, callback);
    if (toSend != null) {
      toSend.writeTo(channel, callback);
    }
  }

  @Override
  public boolean isSendComplete() {
    return (super.isSendComplete()) && (toSend == null || toSend.isSendComplete());
  }

  @Override
  public long sizeInBytes() {
    return super.sizeInBytes() + (Partition_Response_Info_List_Size + partitionResponseInfoSize) + ((toSend == null) ? 0
        : toSend.sizeInBytes());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("GetResponse[");
    if (toSend != null) {
      sb.append("SizeToSend=").append(toSend.sizeInBytes());
    }
    sb.append(" ServerErrorCode=").append(getError());
    if (partitionResponseInfoList != null) {
      sb.append(" PartitionResponseInfoList=").append(partitionResponseInfoList);
    }
    sb.append("]");
    return sb.toString();
  }

  /**
   * @return the current version in which new GetResponse objects are created.
   */
  static short getCurrentVersion() {
    return CURRENT_VERSION;
  }
}
