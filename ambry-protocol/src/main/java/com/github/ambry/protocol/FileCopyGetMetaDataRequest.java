/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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
import javax.annotation.Nonnull;


/**
 * Protocol class representing request to get metadata of a file.
 */
public class FileCopyGetMetaDataRequest extends RequestOrResponse {
  /**
   * The partition id of the file.
   */
  private final PartitionId partitionId;

  /**
   * The hostname of the server.
   */
  private final String hostName;

  /**
   * The version of the request.
   */
  public static final short FILE_METADATA_REQUEST_VERSION_V_1 = 1;

  /**
   * The current version of the response.
   */
  static short CURRENT_VERSION = FILE_METADATA_REQUEST_VERSION_V_1;

  /**
   * The size of the hostname field in bytes.
   */
  private static final int HOST_NAME_FIELD_SIZE_IN_BYTES = 4;

  /**
   * Constructor for FileCopyGetMetaDataRequest
   * @param versionId The version of the request.
   * @param correlationId The correlation id of the request.
   * @param clientId The client id of the request.
   * @param partitionId The partition id of the file.
   * @param hostName The hostname of the server.
   */
  public FileCopyGetMetaDataRequest(
      short versionId,
      int correlationId,
      String clientId,
      @Nonnull PartitionId partitionId,
      @Nonnull String hostName) {
    super(RequestOrResponseType.FileCopyGetMetaDataRequest, versionId, correlationId, clientId);

    Objects.requireNonNull(partitionId, "partitionId must not be null");
    if (hostName.isEmpty()){
      throw new IllegalArgumentException("Host Name cannot be null");
    }
    this.partitionId = partitionId;
    this.hostName = hostName;
  }

  /**
   * Get the hostname of the server.
   */
  public String getHostName() {
    return hostName;
  }

  /**
   * Get the partition id of the file.
   */
  public PartitionId getPartitionId() {
    return partitionId;
  }

  /**
   * Serialize the request into a buffer.
   */
  public static FileCopyGetMetaDataRequest readFrom(
      @Nonnull DataInputStream stream,
      @Nonnull ClusterMap clusterMap) throws IOException {
    Objects.requireNonNull(stream, "stream should not be null");
    Objects.requireNonNull(clusterMap, "clusterMap should not be null");

    short versionId = stream.readShort();
    validateVersion(versionId);

    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    String hostName = Utils.readIntString(stream);
    PartitionId partitionId = clusterMap.getPartitionIdFromStream(stream);

    return new FileCopyGetMetaDataRequest(versionId, correlationId, clientId, partitionId, hostName);
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("FileCopyGetMetaDataRequest[")
      .append("PartitionId=").append(partitionId.getId())
      .append(", HostName=").append(hostName)
      .append("]");
    return sb.toString();
  }

  @Override
  public void accept(RequestVisitor visitor) {
    visitor.visit(this);
  }

  /**
   * Get the size of the request in bytes.
   */
  @Override
  public long sizeInBytes() {
    return super.sizeInBytes() + HOST_NAME_FIELD_SIZE_IN_BYTES + hostName.length() + partitionId.getBytes().length;
  }

  /**
   * Prepare the buffer to be sent over the network.
   */
  @Override
  protected void prepareBuffer() {
    super.prepareBuffer();
    Utils.serializeString(bufferToSend, hostName, Charset.defaultCharset());
    bufferToSend.writeBytes(partitionId.getBytes());
  }

  /**
   * Validate the version of the request.
   */
  static void validateVersion(short version) {
    if (version != CURRENT_VERSION) {
      throw new IllegalArgumentException("Unknown version for FileMetadataRequest: " + version);
    }
  }
}
