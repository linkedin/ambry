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
 * {@link AdminResponse} for {@link CatchupStatusAdminRequest} that provides catchup status.
 */
public class CatchupStatusAdminResponse extends AdminResponse {
  private static final short VERSION_V1 = 1;

  private final boolean isCaughtUp;
  private final long sizeInBytes;

  /**
   * Reads from a stream and constructs a {@link CatchupStatusAdminResponse}.
   * @param stream the stream to read from
   * @return the {@link CatchupStatusAdminResponse} constructed from the {@code stream}.
   * @throws IOException if there is any problem reading from the stream
   */
  public static CatchupStatusAdminResponse readFrom(DataInputStream stream) throws IOException {
    AdminResponse adminResponse = AdminResponse.readFrom(stream);
    Short versionId = stream.readShort();
    if (!versionId.equals(VERSION_V1)) {
      throw new IllegalStateException("Unrecognized version for CatchupStatusAdminResponse: " + versionId);
    }
    return new CatchupStatusAdminResponse(stream.readByte() == 1, adminResponse);
  }

  /**
   * Construct a CatchupStatusAdminResponse
   * @param isCaughtUp {@code true} if replicas have caught up. {@code false} otherwise.
   * @param adminResponse the {@link AdminResponse} that contains common admin response related information.
   */
  public CatchupStatusAdminResponse(boolean isCaughtUp, AdminResponse adminResponse) {
    super(adminResponse.getCorrelationId(), adminResponse.getClientId(), adminResponse.getError());
    this.isCaughtUp = isCaughtUp;
    // parent size + version size + byte size
    sizeInBytes = super.sizeInBytes() + Short.BYTES + Byte.BYTES;
  }

  /**
   * @return {@code true} if replicas have caught up. {@code false} otherwise.
   */
  public boolean isCaughtUp() {
    return isCaughtUp;
  }

  @Override
  public long sizeInBytes() {
    return sizeInBytes;
  }

  @Override
  public String toString() {
    return "CatchupStatusAdminResponse[ClientId=" + clientId + ", CorrelationId=" + correlationId + ", CaughtUp="
        + isCaughtUp + "]";
  }

  @Override
  protected void serializeIntoBuffer() {
    super.serializeIntoBuffer();
    bufferToSend.putShort(VERSION_V1);
    bufferToSend.put(isCaughtUp ? (byte) 1 : 0);
  }
}
