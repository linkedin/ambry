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
  private static final short CURRENT_VERSION = 2;
  private final short numReplicasCaughtUpPerPartition;
  private final long sizeInBytes;
  private final BlobStoreControlRequestType controlRequestType;

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
    stream.readShort();
    short numReplicasCaughtUpPerPartition = stream.readShort();
    BlobStoreControlRequestType controlRequestType = BlobStoreControlRequestType.values()[stream.readByte()];
    return new BlobStoreControlAdminRequest(numReplicasCaughtUpPerPartition, controlRequestType, adminRequest);
  }

  /**
   * Ctor for {@link BlobStoreControlAdminRequest}
   * @param numReplicasCaughtUpPerPartition the number of replicas that have to be within specified threshold per partition.
   * @param controlRequestType the type of control request against blob store, defined in {@link BlobStoreControlRequestType}.
   * @param adminRequest the {@link AdminRequest} that contains common admin request related information.
   */
  public BlobStoreControlAdminRequest(short numReplicasCaughtUpPerPartition,
      BlobStoreControlRequestType controlRequestType, AdminRequest adminRequest) {
    super(AdminRequestOrResponseType.BlobStoreControl, adminRequest.getPartitionId(), adminRequest.getCorrelationId(),
        adminRequest.getClientId());
    this.numReplicasCaughtUpPerPartition = numReplicasCaughtUpPerPartition;
    this.controlRequestType = controlRequestType;
    // parent size + version size + numReplicasCaughtUpPerPartition size + enable flag size
    sizeInBytes = super.sizeInBytes() + Short.BYTES + Short.BYTES + Byte.BYTES;
  }

  /**
   * @return the least number of replicas that have to be within 0 byte for each partition.
   */
  public short getNumReplicasCaughtUpPerPartition() {
    return numReplicasCaughtUpPerPartition;
  }

  public BlobStoreControlRequestType getControlRequestType() {
    return controlRequestType;
  }

  @Override
  public long sizeInBytes() {
    return sizeInBytes;
  }

  @Override
  public String toString() {
    return "BlobStoreControlAdminRequest[ClientId=" + clientId + ", CorrelationId=" + correlationId
        + ", ControlRequestType=" + controlRequestType + ", NumReplicasCaughtUpPerPartition="
        + numReplicasCaughtUpPerPartition + ", PartitionId=" + getPartitionId() + "]";
  }

  @Override
  protected void serializeIntoBuffer() {
    super.serializeIntoBuffer();
    bufferToSend.putShort(CURRENT_VERSION);
    bufferToSend.putShort(numReplicasCaughtUpPerPartition);
    bufferToSend.put((byte) controlRequestType.ordinal());
  }
}
