/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
 *  An admin request used to determine if the peers of a storage node have caught up to it.
 */
public class CatchupStatusAdminRequest extends AdminRequest {
  private static final short VERSION_V1 = 1;

  private final long acceptableLagInBytes;
  private final long sizeInBytes;

  /**
   * Reads from a stream and constructs a {@link CatchupStatusAdminRequest}.
   * @param stream the stream to read from
   * @param adminRequest the {@link AdminRequest} that contains some necessary headers.
   * @return the {@link CatchupStatusAdminRequest} constructed from the {@code stream}.
   * @throws IOException if there is any problem reading from the stream
   */
  public static CatchupStatusAdminRequest readFrom(DataInputStream stream, AdminRequest adminRequest)
      throws IOException {
    Short versionId = stream.readShort();
    if (!versionId.equals(VERSION_V1)) {
      throw new IllegalStateException("Unrecognized version for CatchupStatusAdminRequest: " + versionId);
    }
    long acceptableLagInBytes = stream.readLong();
    return new CatchupStatusAdminRequest(acceptableLagInBytes, adminRequest);
  }

  /**
   * Construct a CatchupStatusAdminRequest
   * @param acceptableLagInBytes the number of bytes that the remote can lag by which is considered OK.
   * @param adminRequest the {@link AdminRequest} that contains common admin request related information.
   */
  public CatchupStatusAdminRequest(long acceptableLagInBytes, AdminRequest adminRequest) {
    super(AdminRequestOrResponseType.CatchupStatus, adminRequest.getPartitionId(), adminRequest.getCorrelationId(),
        adminRequest.getClientId());
    this.acceptableLagInBytes = acceptableLagInBytes;
    // parent size + version size + long size
    sizeInBytes = super.sizeInBytes() + Short.BYTES + Long.BYTES;
  }

  /**
   * @return the number of bytes that the remote can lag by which is considered OK.
   */
  public long getAcceptableLagInBytes() {
    return acceptableLagInBytes;
  }

  @Override
  public long sizeInBytes() {
    return sizeInBytes;
  }

  @Override
  public String toString() {
    return "CatchupStatusAdminRequest[ClientId=" + clientId + ", CorrelationId=" + correlationId
        + ", AcceptableLagInBytes=" + acceptableLagInBytes + ", PartitionId=" + getPartitionId() + "]";
  }

  @Override
  protected void serializeIntoBuffer() {
    super.serializeIntoBuffer();
    bufferToSend.putShort(VERSION_V1);
    bufferToSend.putLong(acceptableLagInBytes);
  }
}
