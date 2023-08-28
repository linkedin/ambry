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

import static com.github.ambry.protocol.RequestOrResponseType.PurgeRequest;


/**
 * Request to purge a blob.
 * This message is sent by the frontend when it has been determined that a chunk is no longer needed (i.e, is expired or
 * deleted and past retention window or maybe an orphan chunk). Once compaction sees a purge message, it will clean up
 * the chunk without any other checks.
 * While this message originates from frontend it can also be replicated.
 */
public class PurgeRequest extends RequestOrResponse {

  static final short PURGE_MESSAGE_REQUEST_VERSION_1 = 1;
  private final static short CURRENT_VERSION = PURGE_MESSAGE_REQUEST_VERSION_1;

  private final BlobId blobId;
  private final long purgeTimeInMs;

  /**
   * Helper to construct PurgeRequest from a stream
   * @param stream the stream to read data from
   * @param map the {@link ClusterMap} to use
   * @return a TtlUpdateRequest based on data read off of the stream
   * @throws IOException if there were any problems reading the stream
   */
  public static PurgeRequest readFrom(DataInputStream stream, ClusterMap map) throws IOException {
    short version = stream.readShort();
    if (version == PURGE_MESSAGE_REQUEST_VERSION_1) {
      return PurgeRequest_V1.readFrom(stream, map);
    }
    throw new IllegalStateException("Unknown Purge Request version " + version);
  }

  /**
   * @param correlationId the correlation id for the request
   * @param clientId the id of the client generating the request
   * @param blobId the blob ID whose TTL needs to be updated
   * @param purgeTimeInMs deletion time of the blob in ms
   */
  public PurgeRequest(int correlationId, String clientId, BlobId blobId, long purgeTimeInMs) {
    this(correlationId, clientId, blobId, purgeTimeInMs, CURRENT_VERSION);
  }

  /**
   * @param correlationId the correlation id for the request
   * @param clientId the id of the client generating the request
   * @param blobId the blob ID that needs to be purged.
   * @param purgeTimeInMs deletion time of the blob in ms
   * @param version the version of the {@link PurgeRequest}.
   */
  PurgeRequest(int correlationId, String clientId, BlobId blobId, long purgeTimeInMs, short version) {
    super(PurgeRequest, version, correlationId, clientId);
    this.blobId = blobId;
    this.purgeTimeInMs = purgeTimeInMs;
  }

  @Override
  public void accept(RequestVisitor visitor) {
    // TODO Efficient_Metadata_Operations_TODO: Implement this.
  }

  @Override
  protected void prepareBuffer() {
    super.prepareBuffer();
    bufferToSend.writeBytes(blobId.toBytes());
  }

  /**
   * @return the blob ID that needs to be purged.
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
   * @return the purge time.
   */
  public long getPurgeTimeInMs() {
    return purgeTimeInMs;
  }

  @Override
  public long sizeInBytes() {
    // header + blobId
    return super.sizeInBytes() + blobId.sizeInBytes();
  }

  @Override
  public String toString() {
    return "PurgeRequest[" + "BlobID=" + blobId + ", " + "PartitionId=" + blobId.getPartition() + ", " + "ClientId="
        + clientId + ", " + "CorrelationId=" + correlationId + ", " + "AccountId=" + blobId.getAccountId() + ", "
        + "ContainerId=" + blobId.getContainerId() + "]";
  }

  /**
   * Class to read protocol version 1 PurgeRequest from the stream.
   */
  public static class PurgeRequest_V1 {
    static PurgeRequest readFrom(DataInputStream stream, ClusterMap map) throws IOException {
      int correlationId = stream.readInt();
      String clientId = Utils.readIntString(stream);
      BlobId id = new BlobId(stream, map);
      long purgeTimeInMs = stream.readLong();
      return new PurgeRequest(correlationId, clientId, id, purgeTimeInMs, PURGE_MESSAGE_REQUEST_VERSION_1);
    }
  }
}
