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


/**
 * Delete request to delete blob
 */
public class DeleteRequest extends RequestOrResponse {
  private final BlobId blobId;
  private final long deletionTimeInMs;
  private final boolean isForceDelete;
  static final short DELETE_REQUEST_VERSION_1 = 1;
  static final short DELETE_REQUEST_VERSION_2 = 2;
  static final short DELETE_REQUEST_VERSION_3 = 3;
  // TODO Update CURRENT_VERSION to 3 after the change is rolled out on all servers
  private final static short CURRENT_VERSION = DELETE_REQUEST_VERSION_2;
  protected static final int DELETION_TIME_FIELD_SIZE_IN_BYTES = Long.BYTES;
  protected static final int FORCE_DELETE_FLAG_SIZE = 1;

  /**
   * Constructs {@link DeleteRequest} in current version
   * @param correlationId correlationId of the delete request
   * @param clientId clientId of the delete request
   * @param blobId blobId of the delete request
   * @param deletionTimeInMs deletion time of the blob in ms
   * @param isForceDelete    {@code true} if we should insert a Delete tombstone even if blob is missing on server.
   *                         Else {@code false}.
   */
  public DeleteRequest(int correlationId, String clientId, BlobId blobId, long deletionTimeInMs,
      boolean isForceDelete) {
    this(correlationId, clientId, blobId, deletionTimeInMs, CURRENT_VERSION, isForceDelete);
  }

  /**
   * Constructs {@link DeleteRequest} in current version
   * @param correlationId correlationId of the delete request
   * @param clientId clientId of the delete request
   * @param blobId blobId of the delete request
   * @param deletionTimeInMs deletion time of the blob in ms
   */
  public DeleteRequest(int correlationId, String clientId, BlobId blobId, long deletionTimeInMs) {
    this(correlationId, clientId, blobId, deletionTimeInMs, false);
  }

  /**
   * Constructs {@link DeleteRequest} in given version.
   *
   * @param correlationId    correlationId of the delete request
   * @param clientId         clientId of the delete request
   * @param blobId           blobId of the delete request
   * @param deletionTimeInMs deletion time of the blob in ms
   * @param version          version of the {@link DeleteRequest}
   * @param isForceDelete    {@code true} if we should insert a Delete tombstone even if blob is missing on server.
   *                         Else {@code false}.
   */
  protected DeleteRequest(int correlationId, String clientId, BlobId blobId, long deletionTimeInMs, short version,
      boolean isForceDelete) {
    super(RequestOrResponseType.DeleteRequest, version, correlationId, clientId);
    this.blobId = blobId;
    this.deletionTimeInMs = deletionTimeInMs;
    this.isForceDelete = isForceDelete;
  }

  public static DeleteRequest readFrom(DataInputStream stream, ClusterMap map) throws IOException {
    Short version = stream.readShort();
    switch (version) {
      case DELETE_REQUEST_VERSION_1:
        return DeleteRequest_V1.readFrom(stream, map);
      case DELETE_REQUEST_VERSION_2:
        return DeleteRequest_V2.readFrom(stream, map);
      case DELETE_REQUEST_VERSION_3:
        return DeleteRequest_V3.readFrom(stream, map);
      default:
        throw new IllegalStateException("Unknown Delete Request version " + version);
    }
  }

  @Override
  public void accept(RequestVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  protected void prepareBuffer() {
    super.prepareBuffer();
    bufferToSend.writeBytes(blobId.toBytes());
    if (versionId == DELETE_REQUEST_VERSION_2) {
      bufferToSend.writeLong(deletionTimeInMs);
    }
    if (versionId == DELETE_REQUEST_VERSION_3) {
      bufferToSend.writeByte(isForceDelete ? (byte) 1 : (byte) 0);
    }
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

  public boolean shouldForceDelete() {
    return isForceDelete;
  }

  @Override
  public long sizeInBytes() {
    // header + blobId
    long sizeInBytes = super.sizeInBytes() + blobId.sizeInBytes();
    if (versionId == DELETE_REQUEST_VERSION_2) {
      // deletion time
      sizeInBytes += DELETION_TIME_FIELD_SIZE_IN_BYTES;
    }
    if (versionId == DELETE_REQUEST_VERSION_3) {
      // Force delete request flag size
      sizeInBytes += FORCE_DELETE_FLAG_SIZE;
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
    sb.append(", ").append("ForceDeleteFlag=").append(isForceDelete);
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
      return new DeleteRequest(correlationId, clientId, id, Utils.Infinite_Time, DELETE_REQUEST_VERSION_1, false);
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
      return new DeleteRequest(correlationId, clientId, id, deletionTimeInMs, DELETE_REQUEST_VERSION_2, false);
    }
  }

  private static class DeleteRequest_V3 {
    static DeleteRequest readFrom(DataInputStream stream, ClusterMap map) throws IOException {
      int correlationId = stream.readInt();
      String clientId = Utils.readIntString(stream);
      BlobId id = new BlobId(stream, map);
      long deletionTimeInMs = stream.readLong();
      boolean isForceDelete = stream.readByte() == 1;
      return new DeleteRequest(correlationId, clientId, id, deletionTimeInMs, DELETE_REQUEST_VERSION_3, isForceDelete);
    }
  }
}
