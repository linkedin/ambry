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

import java.io.DataInputStream;
import java.io.IOException;


/**
 *  An admin request used to stop a BlobStore safely.
 */
public class StopBlobStoreAdminRequest extends AdminRequest {
  private static final short VERSION_V1 = 1;
  private final short numReplicasCaughtUpPerPartition;
  private final long sizeInBytes;

  /**
   * Reads from a stream and constructs a {@link StopBlobStoreAdminRequest}.
   * @param stream the stream to read from
   * @param adminRequest the {@link AdminRequest} that contains some necessary headers.
   * @return the {@link StopBlobStoreAdminRequest} constructed from the {@code stream}.
   * @throws IOException if there is any problem reading from the stream
   */
  public static StopBlobStoreAdminRequest readFrom(DataInputStream stream, AdminRequest adminRequest)
      throws IOException {
    Short versionId = stream.readShort();
    if (!versionId.equals(VERSION_V1)) {
      throw new IllegalStateException("Unrecognized version for StopBlobStoreAdminRequest: " + versionId);
    }
    short numReplicasCaughtUpPerPartition = stream.readShort();
    return new StopBlobStoreAdminRequest(numReplicasCaughtUpPerPartition, adminRequest);
  }

  public StopBlobStoreAdminRequest(short numReplicasCaughtUpPerPartition, AdminRequest adminRequest) {
    super(AdminRequestOrResponseType.StopBlobStore, adminRequest.getPartitionId(), adminRequest.getCorrelationId(),
        adminRequest.getClientId());
    this.numReplicasCaughtUpPerPartition = numReplicasCaughtUpPerPartition;
    // parent size + version size + numReplicasCaughtUpPerPartition size
    sizeInBytes = super.sizeInBytes() + Short.BYTES + +Short.BYTES;
  }

  /**
   * @return the least number of replicas that have to be within 0 byte for each partition.
   */
  public short getNumReplicasCaughtUpPerPartition() {
    return numReplicasCaughtUpPerPartition;
  }

  @Override
  public long sizeInBytes() {
    return sizeInBytes;
  }

  @Override
  public String toString() {
    return "StopBlobStoreAdminRequest[ClientId=" + clientId + ", CorrelationId=" + correlationId
        + ", NumReplicasCaughtUpPerPartition=" + numReplicasCaughtUpPerPartition + ", PartitionId=" + getPartitionId()
        + "]";
  }

  @Override
  protected void serializeIntoBuffer() {
    super.serializeIntoBuffer();
    bufferToSend.putShort(VERSION_V1);
    bufferToSend.putShort(numReplicasCaughtUpPerPartition);
  }
}
