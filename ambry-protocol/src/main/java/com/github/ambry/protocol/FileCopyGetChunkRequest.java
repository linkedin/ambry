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
   * The hostname of the server.
   */
  private final String hostName;

  /**
   * The start offset of the chunk.
   */
  private final long startOffset;

  /**
   * The snapshot id of the partition on serving node.
   * This is returned as part of {@link FileCopyGetMetaDataResponse} and is used to ensure that a bootstrapping request
   * doesn't clash with an ongoing compaction.
   */
  private final String snapshotId;

  /**
   * The size of the chunk in bytes.
   */
  private final long chunkLengthInBytes;

  /**
   * Whether the request is chunked or not.
   */
  private final boolean isChunked;

  /**
   * The version of the FileChunkRequest
   */
  public static final short FILE_CHUNK_REQUEST_VERSION_V_1 = 1;

  /**
   * The size of the file name field in bytes.
   */
  private static final int FILE_NAME_SIZE_IN_BYTES = 4;

  /**
   * The size of the isChunked field in bytes.
   */
  private static final int IS_CHUNKED_SIZE_IN_BYTES = 1;

  /**
   * The current version of the FileChunkRequest
   */
  static short CURRENT_VERSION = FILE_CHUNK_REQUEST_VERSION_V_1;

  /**
   * Constructor for FileCopyGetChunkRequest
   *
   * @param versionId     The version of the request.
   * @param correlationId The correlation id of the request.
   * @param clientId      The client id of the request.
   * @param partitionId   The partition id of the requested file.
   * @param fileName      The name of the requested file.
   * @param hostName      The hostname of requesting server.
   * @param snapshotId    The requested snapshotId.
   * @param startOffset   The start offset of the chunk.
   * @param sizeInBytes   The size of the chunk in bytes.
   */
  public FileCopyGetChunkRequest(short versionId, int correlationId,
      String clientId, PartitionId partitionId, String fileName, String hostName, String snapshotId, long startOffset,
      long sizeInBytes, boolean isChunked) {
    super(RequestOrResponseType.FileCopyGetChunkRequest, versionId, correlationId, clientId);
    validateRequest(versionId, partitionId, fileName, hostName, snapshotId, startOffset, sizeInBytes);

    this.partitionId = partitionId;
    this.fileName = fileName;
    this.hostName = hostName;
    this.startOffset = startOffset;
    this.snapshotId = snapshotId;
    this.chunkLengthInBytes = sizeInBytes;
    this.isChunked = isChunked;
  }

  private void validateRequest(short versionId, PartitionId partitionId, String fileName, String hostName,
      String snapshotId, long startOffset, long sizeInBytes) {
    validateVersion(versionId);
    Objects.requireNonNull(partitionId, "PartitionId cannot be null");
    Objects.requireNonNull(fileName, "FileName cannot be null");
    Objects.requireNonNull(hostName, "HostName cannot be null");
    Objects.requireNonNull(snapshotId, "SnapshotId cannot be null");
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
    String hostName = Utils.readIntString(stream);
    String snapShotId = Utils.readIntString(stream);
    long startOffset = stream.readLong();
    long sizeInBytes = stream.readLong();
    boolean isChunked = stream.readBoolean();

    return new FileCopyGetChunkRequest(versionId, correlationId, clientId, partitionId,
        fileName, hostName, snapShotId, startOffset, sizeInBytes, isChunked);
  }

  /**
   * Prepare the buffer to send
   */
  @Override
  protected void prepareBuffer(){
    super.prepareBuffer();
    bufferToSend.writeBytes(partitionId.getBytes());
    Utils.serializeString(bufferToSend, fileName, Charset.defaultCharset());
    Utils.serializeString(bufferToSend, hostName, Charset.defaultCharset());
    Utils.serializeString(bufferToSend, snapshotId, Charset.defaultCharset());
    bufferToSend.writeLong(startOffset);
    bufferToSend.writeLong(chunkLengthInBytes);
    bufferToSend.writeBoolean(isChunked);
  }

  public String toString(){
    StringBuilder sb = new StringBuilder();
    sb.append("FileCopyProtocolGetChunkRequest[")
        .append("PartitionId=").append(partitionId)
        .append(", FileName=").append(fileName)
        .append(", HostName=").append(hostName)
        .append(", SnapshotId=").append(snapshotId)
        .append(", StartOffset=").append(startOffset)
        .append(", SizeInBytes=").append(chunkLengthInBytes)
        .append(", IsChunked=").append(isChunked)
        .append("]");
    return sb.toString();
  }

  @Override
  public void accept(RequestVisitor visitor) {
    visitor.visit(this);
  }

  /**
   * Get the size of the request in bytes
   * @return long
   */
  public long sizeInBytes() {
    return super.sizeInBytes() + partitionId.getBytes().length + FILE_NAME_SIZE_IN_BYTES + fileName.length() +
        hostName.length() + snapshotId.length() + Long.BYTES + Long.BYTES + IS_CHUNKED_SIZE_IN_BYTES;
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
   * Get the hostname of the server.
   */
  public String getHostName() {
    return hostName;
  }

  /**
   * Get the requested snapshotId.
   */
  public String getSnapshotId() {
    return snapshotId;
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

  /**
   * Get whether the request is chunked or not
   * @return boolean
   */
  public boolean isChunked() {
    return isChunked;
  }

  static void validateVersion(short version){
    if (version != CURRENT_VERSION) {
      throw new IllegalArgumentException("Unknown version for FileMetadataRequest: " + version);
    }
  }
}
