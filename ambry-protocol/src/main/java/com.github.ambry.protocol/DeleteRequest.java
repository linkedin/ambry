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
import com.github.ambry.commons.BlobId;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;


/**
 * Delete request to delete blob
 */
public class DeleteRequest extends RequestOrResponse {
  private final BlobId blobId;
  private final long deletionTimeInMs;
  static final short DELETE_REQUEST_VERSION_1 = 1;
  static final short DELETE_REQUEST_VERSION_2 = 2;
  private final static short CURRENT_VERSION = DELETE_REQUEST_VERSION_2;
  protected static final int DELETION_TIME_FIELD_SIZE_IN_BYTES = Long.BYTES;

  private int sizeSent;

  /**
   * Constructs {@link DeleteRequest} in {@link #DELETE_REQUEST_VERSION_2}
   * @param correlationId correlationId of the delete request
   * @param clientId clientId of the delete request
   * @param blobId blobId of the delete request
   * @param deletionTimeInMs deletion time of the blob in ms
   */
  public DeleteRequest(int correlationId, String clientId, BlobId blobId, long deletionTimeInMs) {
    this(correlationId, clientId, blobId, deletionTimeInMs, CURRENT_VERSION);
  }

  /**
   * Constructs {@link DeleteRequest} in given version.
   * @param correlationId correlationId of the delete request
   * @param clientId clientId of the delete request
   * @param blobId blobId of the delete request
   * @param deletionTimeInMs deletion time of the blob in ms
   * @param version version of the {@link DeleteRequest}
   */
  protected DeleteRequest(int correlationId, String clientId, BlobId blobId, long deletionTimeInMs, short version) {
    super(RequestOrResponseType.DeleteRequest, version, correlationId, clientId);
    this.blobId = blobId;
    this.deletionTimeInMs = deletionTimeInMs;
    sizeSent = 0;
  }

  public static DeleteRequest readFrom(DataInputStream stream, ClusterMap map) throws IOException {
    Short version = stream.readShort();
    switch (version) {
      case DELETE_REQUEST_VERSION_1:
        return DeleteRequest_V1.readFrom(stream, map);
      case DELETE_REQUEST_VERSION_2:
        return DeleteRequest_V2.readFrom(stream, map);
      default:
        throw new IllegalStateException("Unknown Delete Request version " + version);
    }
  }

  @Override
  public long writeTo(WritableByteChannel channel) throws IOException {
    long written = 0;
    if (bufferToSend == null) {
      bufferToSend = ByteBuffer.allocate((int) sizeInBytes());
      writeHeader();
      bufferToSend.put(blobId.toBytes());
      if (versionId == DELETE_REQUEST_VERSION_2) {
        bufferToSend.putLong(deletionTimeInMs);
      }
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

  public BlobId getBlobId() {
    return blobId;
  }

  public short getAccountId() {
    return blobId.getAccountId();
  }

  public short getContainerId() {
    return blobId.getContainerId();
  }

  public long getDeletionTimeInMs() {
    return deletionTimeInMs;
  }

  @Override
  public long sizeInBytes() {
    // header + blobId
    long sizeInBytes = super.sizeInBytes() + blobId.sizeInBytes();
    if (versionId == DELETE_REQUEST_VERSION_2) {
      // deletion time
      sizeInBytes += DELETION_TIME_FIELD_SIZE_IN_BYTES;
    }
    return sizeInBytes;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("DeleteRequest[");
    sb.append("BlobID=").append(blobId);
    sb.append(", ").append("PartitionId=").append(blobId.getPartition());
    sb.append(", ").append("ClientId=").append(clientId);
    sb.append(", ").append("CorrelationId=").append(correlationId);
    sb.append(", ").append("AccountId=").append(blobId.getAccountId());
    sb.append(", ").append("ContainerId=").append(blobId.getContainerId());
    sb.append(", ").append("DeletionTimeInMs=").append(deletionTimeInMs);
    sb.append("]");
    return sb.toString();
  }

  /**
   * Class to read protocol version 1 DeleteRequest from the stream.
   */
  private static class DeleteRequest_V1 {
    static DeleteRequest readFrom(DataInputStream stream, ClusterMap map) throws IOException {
      int correlationId = stream.readInt();
      String clientId = Utils.readIntString(stream);
      BlobId id = new BlobId(stream, map);
      return new DeleteRequest(correlationId, clientId, id, Utils.Infinite_Time, DELETE_REQUEST_VERSION_1);
    }
  }

  /**
   * Class to read protocol version 2 DeleteRequest from the stream.
   */
  private static class DeleteRequest_V2 {
    static DeleteRequest readFrom(DataInputStream stream, ClusterMap map) throws IOException {
      int correlationId = stream.readInt();
      String clientId = Utils.readIntString(stream);
      BlobId id = new BlobId(stream, map);
      long deletionTimeInMs = stream.readLong();
      return new DeleteRequest(correlationId, clientId, id, deletionTimeInMs);
    }
  }
}
