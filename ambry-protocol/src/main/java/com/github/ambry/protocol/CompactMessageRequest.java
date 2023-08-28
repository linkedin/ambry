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
   */
  public CompactMessageRequest(int correlationId, String clientId, BlobId blobId) {
    this(correlationId, clientId, blobId, CURRENT_VERSION);
  }

  /**
   * @param correlationId the correlation id for the request
   * @param clientId the id of the client generating the request
   * @param blobId the blob ID whose TTL needs to be updated
   * @param version the version of the TtlUpdateRequest
   */
  CompactMessageRequest(int correlationId, String clientId, BlobId blobId, short version) {
    super(CompactMessageRequest, version, correlationId, clientId);
    this.blobId = blobId;
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

  @Override
  public long sizeInBytes() {
    // header + blobId
    return super.sizeInBytes() + blobId.sizeInBytes();
  }

  @Override
  public String toString() {
    return "CompactMessageRequest[" + "BlobID=" + blobId + ", " + "PartitionId=" + blobId.getPartition() + ", " + "ClientId="
        + clientId + ", " + "CorrelationId=" + correlationId + ", " + "AccountId=" + blobId.getAccountId() + ", "
        + "ContainerId=" + blobId.getContainerId() + "]";
  }

  /**
   * Class to read protocol version 1 CompactMessageRequest from the stream.
   */
  public static class CompactMessageRequest_V1 {
    static CompactMessageRequest readFrom(DataInputStream stream, ClusterMap map) throws IOException {
      int correlationId = stream.readInt();
      String clientId = Utils.readIntString(stream);
      BlobId id = new BlobId(stream, map);
      return new CompactMessageRequest(correlationId, clientId, id, COMPACT_MESSAGE_REQUEST_VERSION_1);
    }
  }
}
