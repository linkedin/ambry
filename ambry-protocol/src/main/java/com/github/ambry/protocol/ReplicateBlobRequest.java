/**
 * Copyright 2022 LinkedIn Corp. All rights reserved.
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
 * LOCAL_CONSISTENCY_TODO: Rename ReplicateBlobRequest to RepairBlobRequest
 * ReplicateBlob request to replicate one particular blob
 */
public class ReplicateBlobRequest extends RequestOrResponse {
  public final static short VERSION_1 = 1;
  public final static short VERSION_2 = 2;

  // the Blob to be repaired.
  private final BlobId blobId;
  // sourceHostName and sourceHostPort determine the source DataNodeId.
  private final String sourceHostName;
  private final int sourceHostPort;

  // operation related parameters
  private final RequestOrResponseType operationType;
  private final long operationTimeInMs;
  private final short lifeVersion;
  private final long expirationTimeInMs; // It's only used when operationType is RequestOrResponseType.TtlUpdateRequest

  private static final short SOURCE_HOST_NAME_SIZE_IN_BYTES = Integer.BYTES;
  private static final short SOURCE_HOST_PORT_SIZE_IN_BYTES = Integer.BYTES;
  private static final short OPERATION_TYPE_SIZE_IN_BYTES = Short.BYTES;
  private static final short OPERATION_TIME_SIZE_IN_BYTES = Long.BYTES;
  private static final short LIFE_VERSION_SIZE_IN_BYTES = Short.BYTES;
  private static final short OPERATION_PARAMETER_SIZE_IN_BYTES = Long.BYTES;

  /**
   * Constructs {@link ReplicateBlobRequest} in {@link #VERSION_1}
   * @param correlationId correlationId of the ReplicateBlob request
   * @param clientId clientId of the ReplicateBlob request
   * @param blobId blobId of the ReplicateBlob request
   * @param sourceHostName the name of the source host to get the blob from
   * @param sourceHostPort the port of the source host to get the blob from
   */
  public ReplicateBlobRequest(int correlationId, String clientId, BlobId blobId, String sourceHostName,
      int sourceHostPort) {
    this(correlationId, clientId, blobId, sourceHostName, sourceHostPort, VERSION_1, RequestOrResponseType.PutRequest,
        0, (short) -1, 0);
  }

  /**
   * Constructs {@link ReplicateBlobRequest} in {@link #VERSION_2}
   * @param correlationId correlationId of the ReplicateBlob request
   * @param clientId clientId of the ReplicateBlob request
   * @param blobId blobId of the ReplicateBlob request
   * @param sourceHostName the name of the source host to get the blob from
   * @param sourceHostPort the port of the source host to get the blob from
   * @param operationType type of the {@link RequestOrResponseType}
   * @param operationTimeInMs operation time in milliseconds
   * @param lifeVersion life version of the Blob
   * @param expirationTimeInMs expiration time for TtlUpdate
   */
  public ReplicateBlobRequest(int correlationId, String clientId, BlobId blobId, String sourceHostName,
      int sourceHostPort, RequestOrResponseType operationType, long operationTimeInMs, short lifeVersion,
      long expirationTimeInMs) {
    this(correlationId, clientId, blobId, sourceHostName, sourceHostPort, VERSION_2, operationType, operationTimeInMs,
        lifeVersion, expirationTimeInMs);
  }

  /**
   * Constructs {@link ReplicateBlobRequest} in given version.
   * @param correlationId correlationId of the ReplicateBlob request
   * @param clientId clientId of the ReplicateBlob request
   * @param blobId blobId of the ReplicateBlob request
   * @param sourceHostName the name of the source host to get the blob from
   * @param sourceHostPort the port of the source host to get the blob from
   * @param version version of the {@link ReplicateBlobRequest}
   * @param operationType type of the {@link RequestOrResponseType}
   * @param operationTimeInMs operation time in milliseconds
   * @param lifeVersion life version of the Blob
   * @param expirationTimeInMs expiration time for TtlUpdate
   */
  private ReplicateBlobRequest(int correlationId, String clientId, BlobId blobId, String sourceHostName,
      int sourceHostPort, short version, RequestOrResponseType operationType, long operationTimeInMs, short lifeVersion,
      long expirationTimeInMs) {
    super(RequestOrResponseType.ReplicateBlobRequest, version, correlationId, clientId);
    this.blobId = blobId;
    this.sourceHostName = sourceHostName;
    this.sourceHostPort = sourceHostPort;

    this.operationType = operationType;
    this.operationTimeInMs = operationTimeInMs;
    this.lifeVersion = lifeVersion;
    this.expirationTimeInMs = expirationTimeInMs;
  }

  /**
   * Deserialize {@link ReplicateBlobRequest} from a given {@link DataInputStream}.
   * @param stream The stream that contains the serialized bytes.
   * @param map The {@link ClusterMap} to help build {@link BlobId}.
   * @return A deserialized {@link ReplicateBlobRequest}.
   * @throws IOException Any I/O Errors.
   */
  public static ReplicateBlobRequest readFrom(DataInputStream stream, ClusterMap map) throws IOException {
    short version = stream.readShort();
    switch (version) {
      case VERSION_1:
        return ReplicateBlobRequest_V1.readFrom(stream, map);
      case VERSION_2:
        return ReplicateBlobRequest_V2.readFrom(stream, map);
      default:
        throw new IllegalStateException("Unknown ReplicateBlob Request version " + version);
    }
  }

  @Override
  public void accept(RequestVisitor visitor) {
    visitor.visit(this);
  }

  /**
   * Construct the bufferToSend to serialize the request
   */
  @Override
  protected void prepareBuffer() {
    super.prepareBuffer();
    bufferToSend.writeBytes(blobId.toBytes());
    bufferToSend.writeInt(sourceHostName.length());
    bufferToSend.writeBytes(sourceHostName.getBytes());
    bufferToSend.writeInt(sourceHostPort);

    if (versionId == VERSION_2) {
      bufferToSend.writeShort((short) operationType.ordinal());
      bufferToSend.writeLong(operationTimeInMs);
      bufferToSend.writeShort(lifeVersion);
      bufferToSend.writeLong(expirationTimeInMs);
    }
  }

  /**
   * @return the {@link BlobId} associated with the blob in this request.
   */
  public BlobId getBlobId() {
    return blobId;
  }

  /**
   * @return the account Id.
   */
  public short getAccountId() {
    return blobId.getAccountId();
  }

  /**
   * @return the container Id.
   */
  public short getContainerId() {
    return blobId.getContainerId();
  }

  /**
   * @return the name of the source host from where to get the Blob.
   */
  public String getSourceHostName() {
    return sourceHostName;
  }

  /**
   * @return the port of the source host from where to get the Blob.
   */
  public int getSourceHostPort() {
    return sourceHostPort;
  }

  /**
   * @return the operation type to repair.
   */
  public RequestOrResponseType getOperationType() {
    return operationType;
  }

  /**
   * @return the operation time in milliseconds.
   */
  public long getOperationTimeInMs() {
    return operationTimeInMs;
  }

  /**
   * @return the life version
   */
  public short getLifeVersion() {
    return lifeVersion;
  }

  /**
   * @return the expiration time in milli-seconds
   */
  public long getExpirationTimeInMs() {
    return expirationTimeInMs;
  }

  /**
   * @return the size of the serialized ReplicateBlobRequest stream
   */
  @Override
  public long sizeInBytes() {
    // header + blobId + sourceHostName + sourceHostPort
    long sizeInBytes = super.sizeInBytes() + blobId.sizeInBytes();
    sizeInBytes += SOURCE_HOST_NAME_SIZE_IN_BYTES + sourceHostName.length();
    sizeInBytes += SOURCE_HOST_PORT_SIZE_IN_BYTES;

    if (versionId == VERSION_2) {
      // operationType + operationTimeInMs + lifeVersion + expirationTimeInMs
      sizeInBytes += OPERATION_TYPE_SIZE_IN_BYTES;
      sizeInBytes += OPERATION_TIME_SIZE_IN_BYTES;
      sizeInBytes += LIFE_VERSION_SIZE_IN_BYTES;
      sizeInBytes += OPERATION_PARAMETER_SIZE_IN_BYTES;
    }
    return sizeInBytes;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ReplicateBlobRequest[");
    sb.append("BlobID=").append(blobId);
    sb.append(", ").append("PartitionId=").append(blobId.getPartition());
    sb.append(", ").append("ClientId=").append(clientId);
    sb.append(", ").append("CorrelationId=").append(correlationId);
    sb.append(", ").append("AccountId=").append(blobId.getAccountId());
    sb.append(", ").append("ContainerId=").append(blobId.getContainerId());
    sb.append(", ").append("SourceHostName=").append(sourceHostName);
    sb.append(", ").append("SourceHostPort=").append(sourceHostPort);
    if (versionId == VERSION_2) {
      sb.append(", ").append("OperationType=").append(operationType);
      sb.append(", ").append("OperationTimeInMs=").append(operationTimeInMs);
      sb.append(", ").append("LifeVersion=").append(lifeVersion);
      sb.append(", ").append("ExpirationTimeInMs=").append(expirationTimeInMs);
    }
    sb.append("]");
    return sb.toString();
  }

  /**
   * Class to read protocol version 1 ReplicateBlobRequest from the stream.
   */
  private static class ReplicateBlobRequest_V1 {
    static ReplicateBlobRequest readFrom(DataInputStream stream, ClusterMap map) throws IOException {
      int correlationId = stream.readInt();
      String clientId = Utils.readIntString(stream);
      BlobId id = new BlobId(stream, map);
      String sourceHostName = Utils.readIntString(stream);
      int sourceHostPort = stream.readInt();
      return new ReplicateBlobRequest(correlationId, clientId, id, sourceHostName, sourceHostPort);
    }
  }

  /**
   * Class to read protocol version 2 ReplicateBlobRequest from the stream.
   */
  private static class ReplicateBlobRequest_V2 {
    static ReplicateBlobRequest readFrom(DataInputStream stream, ClusterMap map) throws IOException {
      int correlationId = stream.readInt();
      String clientId = Utils.readIntString(stream);
      BlobId id = new BlobId(stream, map);
      String sourceHostName = Utils.readIntString(stream);
      int sourceHostPort = stream.readInt();

      RequestOrResponseType operationType = RequestOrResponseType.values()[stream.readShort()];
      long operationTime = stream.readLong();
      short lifeVersion = stream.readShort();
      long expirationTimeInMs = stream.readLong();
      return new ReplicateBlobRequest(correlationId, clientId, id, sourceHostName, sourceHostPort, operationType,
          operationTime, lifeVersion, expirationTimeInMs);
    }
  }
}
