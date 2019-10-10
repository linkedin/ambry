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
 *  An admin request used to start or stop a BlobStore properly.
 */
public class BlobStoreControlAdminRequest extends AdminRequest {
  private static final short VERSION_V1 = 1;
  private final short numReplicasCaughtUpPerPartition;
  private final long sizeInBytes;
  private final BlobStoreControlAction storeControlAction;

  /**
   * Reads from a stream and constructs a {@link BlobStoreControlAdminRequest}.
   * @param stream the stream to read from
   * @param adminRequest the {@link AdminRequest} that contains some necessary headers.
   * @return the {@link BlobStoreControlAdminRequest} constructed from the {@code stream}.
   * @throws IOException if there is any problem reading from the stream
   */
  public static BlobStoreControlAdminRequest readFrom(DataInputStream stream, AdminRequest adminRequest)
      throws IOException {
    // read the version
    Short versionId = stream.readShort();
    if (!versionId.equals(VERSION_V1)) {
      throw new IllegalStateException("Unrecognized version for BlobStoreControlAdminRequest: " + versionId);
    }
    short numReplicasCaughtUpPerPartition = stream.readShort();
    BlobStoreControlAction controlRequestType = BlobStoreControlAction.values()[stream.readByte()];
    return new BlobStoreControlAdminRequest(numReplicasCaughtUpPerPartition, controlRequestType, adminRequest);
  }

  /**
   * Ctor for {@link BlobStoreControlAdminRequest}
   * @param numReplicasCaughtUpPerPartition the number of replicas that have to be within specified threshold per partition.
   * @param storeControlAction the type of control request against blob store, defined in {@link BlobStoreControlAction}.
   * @param adminRequest the {@link AdminRequest} that contains common admin request related information.
   */
  public BlobStoreControlAdminRequest(short numReplicasCaughtUpPerPartition, BlobStoreControlAction storeControlAction,
      AdminRequest adminRequest) {
    super(AdminRequestOrResponseType.BlobStoreControl, adminRequest.getPartitionId(), adminRequest.getCorrelationId(),
        adminRequest.getClientId());
    this.numReplicasCaughtUpPerPartition = numReplicasCaughtUpPerPartition;
    this.storeControlAction = storeControlAction;
    // parent size + version size + numReplicasCaughtUpPerPartition size + enable flag size
    sizeInBytes = super.sizeInBytes() + Short.BYTES + Short.BYTES + Byte.BYTES;
  }

  /**
   * @return the least number of replicas that have to be within 0 byte for each partition.
   */
  public short getNumReplicasCaughtUpPerPartition() {
    return numReplicasCaughtUpPerPartition;
  }

  public BlobStoreControlAction getStoreControlAction() {
    return storeControlAction;
  }

  @Override
  public long sizeInBytes() {
    return sizeInBytes;
  }

  @Override
  public String toString() {
    return "BlobStoreControlAdminRequest[ClientId=" + clientId + ", CorrelationId=" + correlationId
        + ", BlobStoreControlAction=" + storeControlAction + ", NumReplicasCaughtUpPerPartition="
        + numReplicasCaughtUpPerPartition + ", PartitionId=" + getPartitionId() + "]";
  }

  @Override
  protected void serializeIntoBuffer() {
    super.serializeIntoBuffer();
    bufferToSend.putShort(VERSION_V1);
    bufferToSend.putShort(numReplicasCaughtUpPerPartition);
    bufferToSend.put((byte) storeControlAction.ordinal());
  }
}
