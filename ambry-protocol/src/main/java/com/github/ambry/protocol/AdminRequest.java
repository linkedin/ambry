/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;


/**
 * Representation of an administration request. Types of requests are represented by {@link AdminRequestOrResponseType}.
 */
public class AdminRequest extends RequestOrResponse {
  private static final int REQUEST_TYPE_SIZE = 2;
  private static final int PARTITION_ID_PRESENT_FLAG_SIZE = 1;
  private static final short ADMIN_REQUEST_VERSION_V2 = 2;

  private final AdminRequestOrResponseType type;
  private final PartitionId partitionId;
  private final long sizeInBytes;

  /**
   * Reads from a stream and constructs an {@link AdminRequest}.
   * @param stream the {@link DataInputStream} to read from.
   * @param clusterMap the {@link ClusterMap} in use.
   * @return {@link AdminRequest} that is deserialized from the {@code stream}.
   * @throws IOException if there is an I/O error reading from {@code stream}
   */
  public static AdminRequest readFrom(DataInputStream stream, ClusterMap clusterMap) throws IOException {
    Short versionId = stream.readShort();
    if (!versionId.equals(ADMIN_REQUEST_VERSION_V2)) {
      throw new IllegalStateException("Unrecognized version for AdminRequest: " + versionId);
    }
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    AdminRequestOrResponseType type = AdminRequestOrResponseType.values()[stream.readShort()];
    PartitionId id = null;
    if (stream.readByte() == 1) {
      id = clusterMap.getPartitionIdFromStream(stream);
    }
    return new AdminRequest(type, id, correlationId, clientId);
  }

  /**
   * Constructs an admin request with the given parameters.
   * @param type the type of the request.
   * @param partitionId the {@link PartitionId} that the operation should work on. {@link null} if not applicable.
   * @param correlationId an ID to help match responses to requests.
   * @param clientId the ID of the client.
   */
  public AdminRequest(AdminRequestOrResponseType type, PartitionId partitionId, int correlationId, String clientId) {
    super(RequestOrResponseType.AdminRequest, ADMIN_REQUEST_VERSION_V2, correlationId, clientId);
    this.type = type;
    this.partitionId = partitionId;
    sizeInBytes = computeAndGetSizeInBytes();
  }

  @Override
  protected void prepareBuffer() {
    super.prepareBuffer();
    bufferToSend.writeShort((short) type.ordinal());
    bufferToSend.writeByte(partitionId == null ? (byte) 0 : 1);
    if (partitionId != null) {
      bufferToSend.writeBytes(partitionId.getBytes());
    }
  }

  @Override
  public long sizeInBytes() {
    return sizeInBytes;
  }

  /**
   * @return the type of the request
   */
  public AdminRequestOrResponseType getType() {
    return type;
  }

  /**
   * @return the {@link PartitionId} that the operation will work on. {@link null} if not applicable.
   */
  public PartitionId getPartitionId() {
    return partitionId;
  }

  @Override
  public String toString() {
    return "AdminRequest[ClientId=" + clientId + ", CorrelationId=" + correlationId + ", Type=" + type
        + ", PartitionId=" + partitionId + "]";
  }

  /**
   * @return the size in bytes of the serialized version of the request
   */
  private long computeAndGetSizeInBytes() {
    long size = super.sizeInBytes() + REQUEST_TYPE_SIZE + PARTITION_ID_PRESENT_FLAG_SIZE;
    if (partitionId != null) {
      size += partitionId.getBytes().length;
    }
    return size;
  }
}
