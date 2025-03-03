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
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Objects;


/**
 * Protocol class representing request to get a chunk of a file.
 */
public class FileCopyGetChunkRequest extends RequestOrResponse {
  /**
   * The partition id of the requested file.
   */
  private final PartitionId partitionId;

  /**
   * The name of the requested file.
   */
  private final String fileName;

  /**
   * The start offset of the chunk.
   */
  private final long startOffset;

  /**
   * The size of the chunk in bytes.
   */
  private final long chunkLengthInBytes;

  /**
   * The version of the FileChunkRequest
   */
  public static final short FILE_CHUNK_REQUEST_VERSION_V_1 = 1;

  /**
   * The size of the file name field in bytes.
   */
  private static final int FILE_NAME_SIZE_IN_BYTES = 4;

  /**
   * The current version of the FileChunkRequest
   */
  static short CURRENT_VERSION = FILE_CHUNK_REQUEST_VERSION_V_1;

  /**
   * Constructor for FileCopyGetChunkRequest
   * @param versionId The version of the request.
   * @param correlationId The correlation id of the request.
   * @param clientId The client id of the request.
   * @param partitionId The partition id of the requested file.
   * @param fileName The name of the requested file.
   * @param startOffset The start offset of the chunk.
   * @param sizeInBytes The size of the chunk in bytes.
   */
  public FileCopyGetChunkRequest(short versionId, int correlationId,
      String clientId, PartitionId partitionId, String fileName, long startOffset, long sizeInBytes) {
    super(RequestOrResponseType.FileCopyGetChunkRequest, versionId, correlationId, clientId);
    validateRequest(versionId, partitionId, fileName, startOffset, sizeInBytes);

    this.partitionId = partitionId;
    this.fileName = fileName;
    this.startOffset = startOffset;
    this.chunkLengthInBytes = sizeInBytes;
  }

  private void validateRequest(short versionId, PartitionId partitionId, String fileName, long startOffset, long sizeInBytes) {
    validateVersion(versionId);
    Objects.requireNonNull(partitionId, "PartitionId cannot be null");
    Objects.requireNonNull(fileName, "FileName cannot be null");
    Objects.requireNonNull(startOffset, "StartOffset cannot be null");
    Objects.requireNonNull(sizeInBytes, "SizeInBytes cannot be null");

    if (!fileName.endsWith("_log") && !fileName.endsWith("_index") && !fileName.endsWith("_bloom")) {
      throw new IllegalArgumentException("Invalid file name: " + fileName);
    }
    if (startOffset < 0) {
      throw new IllegalArgumentException("Invalid start offset: " + startOffset);
    }
    if (sizeInBytes <= 0) {
      throw new IllegalArgumentException("Invalid size in bytes: " + sizeInBytes);
    }
  }

  /**
   * Serialize the request into a buffer
   * @param stream The stream to write to
   * @param clusterMap The cluster map
   * @return FileCopyGetChunkRequest
   * @throws IOException
   */
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

  /**
   * Prepare the buffer to send
   */
  @Override
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

  /**
   * Get the size of the request in bytes
   * @return long
   */
  public long sizeInBytes() {
    return super.sizeInBytes() + partitionId.getBytes().length + FILE_NAME_SIZE_IN_BYTES + fileName.length() +
        Long.BYTES + Long.BYTES;
  }

  /**
   * Get the partition id of the request
   * @return PartitionId
   */
  public PartitionId getPartitionId() {
    return partitionId;
  }

  /**
   * Get the name of the requested file
   * @return String
   */
  public String getFileName() {
    return fileName;
  }

  /**
   * Get the start offset of the chunk
   * @return long
   */
  public long getStartOffset() {
    return startOffset;
  }

  /**
   * Get the size of the chunk in bytes
   * @return long
   */
  public long getChunkLengthInBytes() {
    return chunkLengthInBytes;
  }

  static void validateVersion(short version){
    if (version != CURRENT_VERSION) {
      throw new IllegalArgumentException("Unknown version for FileMetadataRequest: " + version);
    }
  }
}
