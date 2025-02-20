/**
 * Copyright 2025 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.Charset;


public class FileCopyGetChunkRequest extends RequestOrResponse {
  private final PartitionId partitionId;
  private final String fileName;
  private final long startOffset;
  private final long chunkLengthInBytes;
  public static final short File_Chunk_Request_Version_V1 = 1;
  private static final int File_Name_Size_In_Bytes = 4;

  static short CURRENT_VERSION = File_Chunk_Request_Version_V1;


  public FileCopyGetChunkRequest( short versionId, int correlationId,
      String clientId, PartitionId partitionId, String fileName, long startOffset, long sizeInBytes) {
    super(RequestOrResponseType.FileCopyGetChunkRequest, versionId, correlationId, clientId);
    if(partitionId == null || fileName.isEmpty() || startOffset < 0 || sizeInBytes < 0){
      throw new IllegalArgumentException("PartitionId, FileName, StartOffset or SizeInBytes cannot be null or negative");
    }
    this.partitionId = partitionId;
    this.fileName = fileName;
    this.startOffset = startOffset;
    this.chunkLengthInBytes = sizeInBytes;
  }

  public static FileCopyGetChunkRequest readFrom(DataInputStream stream, ClusterMap clusterMap)
      throws IOException {
    Short versionId = stream.readShort();
    validateVersion(versionId);

    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    PartitionId partitionId = clusterMap.getPartitionIdFromStream(stream);
    String fileName = Utils.readIntString(stream);
    long startOffset = stream.readLong();
    long sizeInBytes = stream.readLong();

    return new FileCopyGetChunkRequest(versionId, correlationId, clientId, partitionId,
        fileName, startOffset, sizeInBytes);
  }

  protected void prepareBuffer(){
    super.prepareBuffer();
    bufferToSend.writeBytes(partitionId.getBytes());
    Utils.serializeString(bufferToSend, fileName, Charset.defaultCharset());
    bufferToSend.writeLong(startOffset);
    bufferToSend.writeLong(chunkLengthInBytes);
  }

  public String toString(){
    StringBuilder sb = new StringBuilder();
    sb.append("FileCopyProtocolGetChunkRequest[")
        .append("PartitionId=").append(partitionId)
        .append(", FileName=").append(fileName)
        .append(", StartOffset=").append(startOffset)
        .append(", SizeInBytes=").append(chunkLengthInBytes)
        .append("]");
    return sb.toString();
  }

  public long sizeInBytes() {
    return super.sizeInBytes() + partitionId.getBytes().length + File_Name_Size_In_Bytes + fileName.length() +
        Long.BYTES + Long.BYTES;
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

  public long getChunkLengthInBytes() {
    return chunkLengthInBytes;
  }

  static void validateVersion(short version){
    if (version != CURRENT_VERSION) {
      throw new IllegalArgumentException("Unknown version for FileMetadataRequest: " + version);
    }
  }
}
