///**
// * Copyright 2024 LinkedIn Corp. All rights reserved.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// */
package com.github.ambry.protocol;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import io.netty.buffer.CompositeByteBuf;

/**
 * This class utilizes distinct data structures to manage chunk data on the sender and receiver nodes.
 *
 * On the sender node, chunk data is stored in a Netty ByteBuf. A composite buffer is employed to combine
 * chunk data and metadata into a single buffer, minimizing in-memory copying of file chunks.
 * On the receiver node, chunk data is stored in a DataInputStream, allowing direct writing
 * to disk without the need for intermediate in-memory copying.
 *
 * This design eliminates the need for redundant byte buffer copies on both sender and receiver nodes,
 * enhancing efficiency and reducing memory overhead.
 */
public class FileCopyGetChunkResponse2 extends Response {
  private final PartitionId partitionId;
  private final String fileName;
  protected DataInputStream chunkDataOnReceiverNode;
  protected ByteBuf chunkDataOnSenderNode;
  private final long startOffset;
  private final long chunkSizeInBytes;
  private final boolean isLastChunk;
  private static final int File_Name_Field_Size_In_Bytes = 4;
  public static final short File_Copy_Chunk_Response_Version_V1 = 1;

  static short CURRENT_VERSION = File_Copy_Chunk_Response_Version_V1;

  public FileCopyGetChunkResponse2(short versionId, int correlationId, String clientId, ServerErrorCode errorCode,
      PartitionId partitionId, String fileName, DataInputStream chunkDataOnReceiverNode, ByteBuf chunkDataOnSenderNode,
      long startOffset, long chunkSizeInBytes, boolean isLastChunk) {
    super(RequestOrResponseType.FileCopyGetChunkResponse, versionId, correlationId, clientId, errorCode);

    validateVersion(versionId);

    this.partitionId = partitionId;
    this.fileName = fileName;
    this.chunkDataOnReceiverNode = chunkDataOnReceiverNode;
    this.chunkDataOnSenderNode = chunkDataOnSenderNode;
    this.startOffset = startOffset;
    this.chunkSizeInBytes = chunkSizeInBytes;
    this.isLastChunk = isLastChunk;
  }

  public FileCopyGetChunkResponse2(int correlationId, String clientId, ServerErrorCode errorCode) {
    this(CURRENT_VERSION, correlationId, clientId, errorCode,
        null, null, null, null,
        -1, -1, false);
  }

  public static FileCopyGetChunkResponse2 readFrom(DataInputStream stream, ClusterMap clusterMap) throws IOException {
    RequestOrResponseType type = RequestOrResponseType.values()[stream.readShort()];
    if (type != RequestOrResponseType.FileCopyGetChunkResponse) {
      throw new IllegalArgumentException("The type of request response is not compatible");
    }
    short versionId = stream.readShort();
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    ServerErrorCode errorCode = ServerErrorCode.values()[stream.readShort()];

    if(errorCode != ServerErrorCode.No_Error){
      // Setting the partitionId and fileName to null as there are no logfiles to be read.
      // Setting the startOffset and sizeInBytes to -1 as there are no logfiles to be read.
      return new FileCopyGetChunkResponse2(versionId, correlationId, clientId, errorCode,
          null, null, null, null,
          -1, -1, false);
    }
    PartitionId partitionId = clusterMap.getPartitionIdFromStream(stream);
    String fileName = Utils.readIntString(stream);
    long startOffset = stream.readLong();
    long sizeInBytes = stream.readLong();
    boolean isLastChunk = stream.readBoolean();
    return new FileCopyGetChunkResponse2(versionId, correlationId, clientId, errorCode, partitionId, fileName, stream, null, startOffset, sizeInBytes, isLastChunk);
  }

  public long sizeInBytes() {
    return super.sizeInBytes() + partitionId.getBytes().length + File_Name_Field_Size_In_Bytes +
        fileName.length() + Long.BYTES + Long.BYTES + 1 + Integer.BYTES + chunkDataOnSenderNode.readableBytes();
  }

  public void prepareBuffer(){
    CompositeByteBuf compositeByteBuf = Unpooled.compositeBuffer(2);
    super.prepareBuffer();
    bufferToSend.writeBytes(partitionId.getBytes());
    Utils.serializeString(bufferToSend, fileName, Charset.defaultCharset());
    bufferToSend.writeLong(startOffset);
    bufferToSend.writeLong(chunkSizeInBytes);
    bufferToSend.writeBoolean(isLastChunk);
    bufferToSend.writeInt(chunkDataOnSenderNode.readableBytes());
    compositeByteBuf.addComponent(true, bufferToSend);
    if(chunkSizeInBytes > 0 ) {
      compositeByteBuf.addComponent(true, chunkDataOnSenderNode);
    }
    bufferToSend = compositeByteBuf;
  }

  public PartitionId getPartitionId() {
    return partitionId;
  }

  public String getFileName() {
    return fileName;
  }

  public long getStartOffset() {
    return startOffset;
  }

  public long getChunkSizeInBytes() {
    return chunkSizeInBytes;
  }

  public boolean isLastChunk() {
    return isLastChunk;
  }

  public DataInputStream getChunkDataOnReceiverNode() {
    return chunkDataOnReceiverNode;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("FileCopyGetChunkResponse[")
      .append("PartitionId=").append(partitionId.getId())
      .append(", FileName=").append(fileName)
      .append(", SizeInBytes=").append(sizeInBytes())
      .append("]");
    return sb.toString();
  }

  private static void validateVersion(short version) {
    if (version != CURRENT_VERSION) {
      throw new IllegalArgumentException("Unknown version for FileCopyGetChunkResponse: " + version);
    }
  }
}
