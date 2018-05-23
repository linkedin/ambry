/*
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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

import static com.github.ambry.protocol.RequestOrResponseType.TtlUpdateRequest;


/**
 * Request to update the TTL of a blob
 */
public class TtlUpdateRequest extends RequestOrResponse {

  static final short TTL_UPDATE_REQUEST_VERSION_1 = 1;
  private final static short CURRENT_VERSION = TTL_UPDATE_REQUEST_VERSION_1;

  private final BlobId blobId;
  private final long expiresAtMs;
  private final long operationTimeInMs;

  private int sizeSent;

  /**
   * Helper to construct TtlUpdateRequest from a stream
   * @param stream the stream to read data from
   * @param map the {@link ClusterMap} to use
   * @return a TtlUpdateRequest based on data read off of the stream
   * @throws IOException if there were any problems reading the stream
   */
  public static TtlUpdateRequest readFrom(DataInputStream stream, ClusterMap map) throws IOException {
    Short version = stream.readShort();
    switch (version) {
      case TTL_UPDATE_REQUEST_VERSION_1:
        return TtlUpdateRequest_V1.readFrom(stream, map);
      default:
        throw new IllegalStateException("Unknown TTL Request version " + version);
    }
  }

  /**
   * @param correlationId the correlation id for the request
   * @param clientId the id of the client generating the request
   * @param blobId the blob ID whose TTL needs to be updated
   * @param expiresAtMs the absolute time (in ms) at which the blob must expire
   * @param operationTimeMs the time of the operation (in ms)
   */
  public TtlUpdateRequest(int correlationId, String clientId, BlobId blobId, long expiresAtMs, long operationTimeMs) {
    this(correlationId, clientId, blobId, expiresAtMs, operationTimeMs, CURRENT_VERSION);
  }

  /**
   * @param correlationId the correlation id for the request
   * @param clientId the id of the client generating the request
   * @param blobId the blob ID whose TTL needs to be updated
   * @param expiresAtMs the absolute time (in ms) at which the blob must expire
   * @param operationTimeMs the time of the operation (in ms)
   * @param version the version of the TtlUpdateRequest
   */
  TtlUpdateRequest(int correlationId, String clientId, BlobId blobId, long expiresAtMs, long operationTimeMs,
      short version) {
    super(TtlUpdateRequest, version, correlationId, clientId);
    this.blobId = blobId;
    this.expiresAtMs = expiresAtMs;
    this.operationTimeInMs = operationTimeMs;
    sizeSent = 0;
  }

  @Override
  public long writeTo(WritableByteChannel channel) throws IOException {
    long written = 0;
    if (bufferToSend == null) {
      bufferToSend = ByteBuffer.allocate((int) sizeInBytes());
      writeHeader();
      bufferToSend.put(blobId.toBytes());
      bufferToSend.putLong(expiresAtMs);
      bufferToSend.putLong(operationTimeInMs);
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
   * @return the absolute time (in ms) at which the blob must expire
   */
  public long getExpiresAtMs() {
    return expiresAtMs;
  }

  /**
   * @return the time of the operation (in ms)
   */
  public long getOperationTimeInMs() {
    return operationTimeInMs;
  }

  @Override
  public long sizeInBytes() {
    // header + blobId
    // expires time ms
    // op time ms
    return super.sizeInBytes() + blobId.sizeInBytes() + 2 * Long.BYTES;
  }

  @Override
  public String toString() {
    return "TtlUpdateRequest[" + "BlobID=" + blobId + ", " + "PartitionId=" + blobId.getPartition() + ", " + "ClientId="
        + clientId + ", " + "CorrelationId=" + correlationId + ", " + "AccountId=" + blobId.getAccountId() + ", "
        + "ContainerId=" + blobId.getContainerId() + ", " + "ExpiresAtMs=" + expiresAtMs + ", " + "OperationTimeMs="
        + operationTimeInMs + "]";
  }

  /**
   * Class to read protocol version 1 TtlUpdateRequest from the stream.
   */
  private static class TtlUpdateRequest_V1 {
    static TtlUpdateRequest readFrom(DataInputStream stream, ClusterMap map) throws IOException {
      int correlationId = stream.readInt();
      String clientId = Utils.readIntString(stream);
      BlobId id = new BlobId(stream, map);
      long expiresAtMs = stream.readLong();
      long operationTimeMs = stream.readLong();
      return new TtlUpdateRequest(correlationId, clientId, id, expiresAtMs, operationTimeMs,
          TTL_UPDATE_REQUEST_VERSION_1);
    }
  }
}
