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


/**
 * Response to a {@link FileCopyGetChunkRequest} that contains the chunk of the file requested.
 * The response contains the chunk of the file requested, the start offset of the chunk, the size of the chunk and whether
 * this is the last chunk of the file.
 */
public class FileCopyGetChunkResponse extends Response {
  /**
   * The chunk stream of the file requested.
   */
  private final DataInputStream chunkStream;

  /**
   * Whether this is the last chunk of the file.
   */
  private final boolean isLastChunk;

  /**
   * The start offset of the chunk.
   */
  private final long startOffset;

  /**
   * The size of the chunk in bytes.
   */
  private final long chunkSizeInBytes;

  /**
   * The partition id of the requested file.
   */
  private final PartitionId partitionId;

  /**
   * The name of the requested file.
   */
  private final String fileName;

  /**
   * The size of the file name field in bytes.
   */
  private static final int FILE_NAME_FIELD_SIZE_IN_BYTES = 4;

  /**
   * The version of the response.
   */
  public static final short FILE_COPY_CHUNK_RESPONSE_VERSION_V_1 = 1;

  /**
   * The current version of the response.
   */
  static short CURRENT_VERSION = FILE_COPY_CHUNK_RESPONSE_VERSION_V_1;

  /**
   * Constructor for FileCopyGetChunkResponse
   * @param versionId The version of the response.
   * @param correlationId The correlation id of the response.
   * @param clientId The client id of the response.
   * @param errorCode The error code of the response.
   * @param partitionId The partition id of the requested file.
   * @param fileName The name of the requested file.
   * @param chunkStream The chunk stream of the file requested.
   * @param startOffset The start offset of the chunk.
   * @param chunkSizeInBytes The size of the chunk in bytes.
   * @param isLastChunk Whether this is the last chunk of the file.
   */
  public FileCopyGetChunkResponse(short versionId, int correlationId, String clientId, ServerErrorCode errorCode,
      PartitionId partitionId, String fileName, DataInputStream chunkStream,
      long startOffset, long chunkSizeInBytes, boolean isLastChunk) {
    super(RequestOrResponseType.FileCopyGetChunkResponse, versionId, correlationId, clientId, errorCode);

    validateVersion(versionId);

    this.chunkStream = chunkStream;
    this.startOffset = startOffset;
    this.chunkSizeInBytes = chunkSizeInBytes;
    this.isLastChunk = isLastChunk;
    this.partitionId = partitionId;
    this.fileName = fileName;
  }

  /**
   * Constructor for FileCopyGetChunkResponse
   * @param correlationId The correlation id of the response.
   * @param clientId The client id of the response.
   * @param errorCode The error code of the response.
   */
  public FileCopyGetChunkResponse(int correlationId, String clientId, ServerErrorCode errorCode) {
    this(CURRENT_VERSION, correlationId, clientId, errorCode, null, null, null, -1, -1, false);
  }

  /**
   * Constructor for FileCopyGetChunkResponse
   * @param serverErrorCode The error code of the response.
   */
  public FileCopyGetChunkResponse(ServerErrorCode serverErrorCode) {
    this(-1, "", serverErrorCode);
  }

  /**
   * Get the partition id of the requested file.
   * @return PartitionId
   */
  public PartitionId getPartitionId() {
    return partitionId;
  }

  /**
   * Get the name of the requested file.
   * @return String
   */
  public String getFileName() {
    return fileName;
  }

  /**
   * Get the start offset of the chunk.
   * @return long
   */
  public long getStartOffset() {
    return startOffset;
  }

  /**
   * Get whether this is the last chunk of the file.
   * @return boolean
   */
  public boolean isLastChunk() {
    return isLastChunk;
  }

  /**
   * Get the size of the chunk in bytes.
   * @return long
   */
  public long getChunkSizeInBytes() {
    return chunkSizeInBytes;
  }

  /**
   * Get the chunk stream of the file requested.
   * @return
   */
  public DataInputStream getChunkStream() {
    return chunkStream;
  }

  /**
   * Get the size of the response in bytes.
   * @return long
   */
  public long sizeInBytes() {
    try {
      return super.sizeInBytes() + partitionId.getBytes().length + FILE_NAME_FIELD_SIZE_IN_BYTES +
          fileName.length() + Long.BYTES + Long.BYTES + 1 + Integer.BYTES +
          (chunkStream != null ? chunkStream.available() : 0);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Serialize the response into a buffer.
   * @param chunkResponseStream The output stream.
   * @param clusterMap The cluster map.
   * @return FileCopyGetChunkResponse
   * @throws IOException If an I/O error occurs.
   */
  public static FileCopyGetChunkResponse readFrom(DataInputStream chunkResponseStream, ClusterMap clusterMap) throws IOException {
    RequestOrResponseType type = RequestOrResponseType.values()[chunkResponseStream.readShort()];
    if (type != RequestOrResponseType.FileCopyGetChunkResponse) {
      throw new IllegalArgumentException("The type of request response is not compatible");
    }
    short versionId = chunkResponseStream.readShort();
    validateVersion(versionId);

    int correlationId = chunkResponseStream.readInt();
    String clientId = Utils.readIntString(chunkResponseStream);

    ServerErrorCode errorCode = ServerErrorCode.values()[chunkResponseStream.readShort()];
    if(errorCode != ServerErrorCode.No_Error){
      return new FileCopyGetChunkResponse(correlationId, clientId, errorCode);
    }
    PartitionId partitionId = clusterMap.getPartitionIdFromStream(chunkResponseStream);
    String fileName = Utils.readIntString(chunkResponseStream);
    long startOffset = chunkResponseStream.readLong();
    long sizeInBytes = chunkResponseStream.readLong();
    boolean isLastChunk = chunkResponseStream.readBoolean();

    return new FileCopyGetChunkResponse(versionId, correlationId, clientId, errorCode, partitionId, fileName,
        chunkResponseStream, startOffset, sizeInBytes, isLastChunk);
  }

  /**
   * Write the response to the buffer.
   */
  @Override
  public void prepareBuffer(){
    super.prepareBuffer();
    bufferToSend.writeBytes(partitionId.getBytes());
    Utils.serializeString(bufferToSend, fileName, Charset.defaultCharset());
    bufferToSend.writeLong(startOffset);
    bufferToSend.writeLong(chunkSizeInBytes);
    bufferToSend.writeBoolean(isLastChunk);
    try {
      bufferToSend.writeBytes(chunkStream, chunkStream.available());
    } catch (IOException e) {
      logger.info("Error while writing chunkStream", e);
      throw new RuntimeException(e);
    }
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    try {
      sb.append("FileCopyGetChunkResponse[")
          .append("PartitionId=").append(partitionId.getId())
          .append(", FileName=").append(fileName)
          .append(", SizeInBytes=").append(chunkStream.available())
          .append(", isLastChunk=").append(isLastChunk)
          .append("]");
    } catch (IOException e) {
      logger.info("Error while reading chunkStream", e);
      throw new RuntimeException(e);
    }
    return sb.toString();
  }

  private static void validateVersion(short version) {
    if (version != CURRENT_VERSION) {
      throw new IllegalArgumentException("Unknown version for FileCopyGetChunkResponse: " + version);
    }
  }
}
