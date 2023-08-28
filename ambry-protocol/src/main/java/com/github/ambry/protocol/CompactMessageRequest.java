/*
 * Copyright 2023 LinkedIn Corp. All rights reserved.
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

import static com.github.ambry.protocol.RequestOrResponseType.CompactMessageRequest;


public class CompactMessageRequest extends RequestOrResponse {

  static final short COMPACT_MESSAGE_REQUEST_VERSION_1 = 1;
  private final static short CURRENT_VERSION = COMPACT_MESSAGE_REQUEST_VERSION_1;

  private final BlobId blobId;
  private final long operationTimeInMs;

  /**
   * Helper to construct TtlUpdateRequest from a stream
   * @param stream the stream to read data from
   * @param map the {@link ClusterMap} to use
   * @return a TtlUpdateRequest based on data read off of the stream
   * @throws IOException if there were any problems reading the stream
   */
  public static CompactMessageRequest readFrom(DataInputStream stream, ClusterMap map) throws IOException {
    short version = stream.readShort();
    if (version == COMPACT_MESSAGE_REQUEST_VERSION_1) {
      return CompactMessageRequest_V1.readFrom(stream, map);
    }
    throw new IllegalStateException("Unknown TTL Request version " + version);
  }

  /**
   * @param correlationId the correlation id for the request
   * @param clientId the id of the client generating the request
   * @param blobId the blob ID whose TTL needs to be updated
   * @param operationTimeMs the time of the operation (in ms)
   */
  public CompactMessageRequest(int correlationId, String clientId, BlobId blobId, long operationTimeMs) {
    this(correlationId, clientId, blobId, operationTimeMs, CURRENT_VERSION);
  }

  /**
   * @param correlationId the correlation id for the request
   * @param clientId the id of the client generating the request
   * @param blobId the blob ID whose TTL needs to be updated
   * @param operationTimeMs the time of the operation (in ms)
   * @param version the version of the TtlUpdateRequest
   */
  CompactMessageRequest(int correlationId, String clientId, BlobId blobId, long operationTimeMs, short version) {
    super(CompactMessageRequest, version, correlationId, clientId);
    this.blobId = blobId;
    this.operationTimeInMs = operationTimeMs;
  }

  @Override
  public void accept(RequestVisitor visitor) {
    // TODO Efficient Metadata Operations: Implement this.
  }

  @Override
  protected void prepareBuffer() {
    super.prepareBuffer();
    bufferToSend.writeBytes(blobId.toBytes());
  }

  /**
   * @return the blob ID whose TTL needs to be updated.
   */
  public BlobId getBlobId() {
    return blobId;
  }

  /**
   * @return the id of the account that the blob belongs to
   */
  public short getAccountId() {
    return blobId.getAccountId();
  }

  /**
   * @return the id of the container that the blob belongs to
   */
  public short getContainerId() {
    return blobId.getContainerId();
  }

  /**
   * @return the time of the operation (in ms)
   */
  public long getOperationTimeInMs() {
    return operationTimeInMs;
  }

  @Override
  public long sizeInBytes() {
    // header + blobId + op time ms
    return super.sizeInBytes() + blobId.sizeInBytes() + Long.BYTES;
  }

  @Override
  public String toString() {
    return "CompactMessageRequest[" + "BlobID=" + blobId + ", " + "PartitionId=" + blobId.getPartition() + ", " + "ClientId="
        + clientId + ", " + "CorrelationId=" + correlationId + ", " + "AccountId=" + blobId.getAccountId() + ", "
        + "ContainerId=" + blobId.getContainerId() + "OperationTimeMs=" + operationTimeInMs + "]";
  }

  /**
   * Class to read protocol version 1 CompactMessageRequest from the stream.
   */
  public static class CompactMessageRequest_V1 {
    static CompactMessageRequest readFrom(DataInputStream stream, ClusterMap map) throws IOException {
      int correlationId = stream.readInt();
      String clientId = Utils.readIntString(stream);
      BlobId id = new BlobId(stream, map);
      long operationTimeMs = stream.readLong();
      return new CompactMessageRequest(correlationId, clientId, id, operationTimeMs, COMPACT_MESSAGE_REQUEST_VERSION_1);
    }
  }
}
