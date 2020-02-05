/**
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;


/**
 * Undelete request to undelete a deleted blob.
 */
public class UndeleteRequest extends RequestOrResponse {
  static final short UNDELETE_REQUEST_VERSION_1 = 1;
  private final static short CURRENT_VERSION = UNDELETE_REQUEST_VERSION_1;

  private int sizeSent = 0;
  private final BlobId blobId;
  private final long operationTimeMs;

  /**
   * Constructs {@link UndeleteRequest} in {@link #UNDELETE_REQUEST_VERSION_1}.
   * @param correlationId correlationId of the undelete request
   * @param clientId clientId of the undelete request
   * @param blobId blobId of the undelete request
   * @param operationTimeMs the time when this operation is created (in ms).
   */
  public UndeleteRequest(int correlationId, String clientId, BlobId blobId, long operationTimeMs) {
    this(correlationId, clientId, blobId, operationTimeMs, CURRENT_VERSION);
  }

  /**
   * Constructs {@link UndeleteRequest} in given version.
   * @param correlationId correlationId of the undelete request
   * @param clientId clientId of the undelete request
   * @param blobId blobId of the undelete request
   * @param operationTimeMs the time when this operation is created (in ms).
   * @param version version of the {@link UndeleteRequest}.
   */
  public UndeleteRequest(int correlationId, String clientId, BlobId blobId, long operationTimeMs, short version) {
    super(RequestOrResponseType.UndeleteRequest, version, correlationId, clientId);
    this.blobId = blobId;
    this.operationTimeMs = operationTimeMs;
    sizeSent = 0;
  }

  public static UndeleteRequest readFrom(DataInputStream stream, ClusterMap map) throws IOException {
    Short version = stream.readShort();
    if (version != UNDELETE_REQUEST_VERSION_1) {
      throw new IllegalStateException("Unknown undelete request version " + version);
    }
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    BlobId id = new BlobId(stream, map);
    long operationTimeMs = stream.readLong();
    return new UndeleteRequest(correlationId, clientId, id, operationTimeMs);
  }

  @Override
  public long writeTo(WritableByteChannel channel) throws IOException {
    long written = 0;
    if (bufferToSend == null) {
      bufferToSend = ByteBuffer.allocate((int) sizeInBytes());
      writeHeader();
      bufferToSend.put(blobId.toBytes());
      bufferToSend.putLong(operationTimeMs);
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
    // header + blobId
    return super.sizeInBytes() + blobId.sizeInBytes() + Long.BYTES;
  }

  /**
   * Return blob id.
   * @return The {@link BlobId}.
   */
  public BlobId getBlobId() {
    return blobId;
  }

  /**
   * Return the account id for the blob to be undeleted.
   * @return the account ID.
   */
  public short getAccountId() {
    return blobId.getAccountId();
  }

  /**
   * Return the container id for the blob to be undeleted.
   * @return the container id.
   */
  public short getContainerId() {
    return blobId.getContainerId();
  }

  /**
   * Return the operationTimeMs.
   * @return the operationTimeMs.
   */
  public long getOperationTimeMs() {
    return operationTimeMs;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("UndeleteRequest[");
    sb.append("BlobID=").append(blobId);
    sb.append(", ").append("PartitionId=").append(blobId.getPartition());
    sb.append(", ").append("ClientId=").append(clientId);
    sb.append(", ").append("CorrelationId=").append(correlationId);
    sb.append(", ").append("AccountId=").append(blobId.getAccountId());
    sb.append(", ").append("ContainerId=").append(blobId.getContainerId());
    sb.append(", ").append("OperationTimeMs=").append(operationTimeMs);
    sb.append("]");
    return sb.toString();
  }
}
