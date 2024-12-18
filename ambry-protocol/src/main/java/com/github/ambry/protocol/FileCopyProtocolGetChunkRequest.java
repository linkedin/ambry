/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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


public class FileCopyProtocolGetChunkRequest extends RequestOrResponse{
  private PartitionId partitionId;
  private String fileName;
  private long startOffset;
  private long sizeInBytes;
  private static final short File_Chunk_Request_Version_V1 = 1;
  private static final int File_Name_Size_In_Bytes = 4;


  public FileCopyProtocolGetChunkRequest( short versionId, int correlationId,
      String clientId, PartitionId partitionId, String fileName, long startOffset, long sizeInBytes) {
    super(RequestOrResponseType.FileCopyProtocolGetChunkRequest, versionId, correlationId, clientId);
    if(partitionId == null || fileName.isEmpty() || startOffset < 0 || sizeInBytes < 0){
      throw new IllegalArgumentException("PartitionId, FileName, StartOffset and SizeInBytes cannot be null or negative");
    }
    this.partitionId = partitionId;
    this.fileName = fileName;
    this.startOffset = startOffset;
    this.sizeInBytes = sizeInBytes;
  }

  public static FileCopyProtocolGetChunkRequest readFrom(DataInputStream stream, ClusterMap clusterMap)
      throws IOException {
    Short versionId = stream.readShort();
    validateVersion(versionId);
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    PartitionId partitionId = clusterMap.getPartitionIdFromStream(stream);
    String fileName = Utils.readIntString(stream);
    long startOffset = stream.readLong();
    long sizeInBytes = stream.readLong();
    return new FileCopyProtocolGetChunkRequest(versionId, correlationId, clientId, partitionId, fileName, startOffset, sizeInBytes);
  }

  protected void prepareBuffer(){
    super.prepareBuffer();
    bufferToSend.writeBytes(partitionId.getBytes());
    Utils.serializeString(bufferToSend, fileName, Charset.defaultCharset());
    bufferToSend.writeLong(startOffset);
    bufferToSend.writeLong(sizeInBytes);
  }

  public String toString(){
    StringBuilder sb = new StringBuilder();
    sb.append("FileCopyProtocolGetChunkRequest[")
      .append("PartitionId=").append(partitionId)
      .append(", FileName=").append(fileName)
      .append(", StartOffset=").append(startOffset)
      .append(", SizeInBytes=").append(sizeInBytes)
      .append("]");
    return sb.toString();
  }

  public long sizeInBytes() {
    return super.sizeInBytes() + partitionId.getBytes().length + File_Name_Size_In_Bytes + fileName.length() + Long.BYTES + Long.BYTES;
  }

  public PartitionId getPartitionId() { return partitionId; }
  public String getFileName() { return fileName; }
  public long getStartOffset() { return startOffset; }
  public long getSizeInBytes() { return sizeInBytes; }

  static void validateVersion(short version){
    if (version != File_Chunk_Request_Version_V1) {
      throw new IllegalArgumentException("Unknown version for FileMetadataRequest: " + version);
    }
  }
}
