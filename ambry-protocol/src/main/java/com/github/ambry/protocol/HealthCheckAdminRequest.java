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

import java.io.DataInputStream;
import java.io.IOException;

/**
 *  An admin request used to query the health status of a server host.
 */
public class HealthCheckAdminRequest extends AdminRequest {
  private static final short VERSION_1 = 1;
  private final long sizeInBytes;

  /**
   * Reads from a stream and constructs a {@link HealthCheckAdminRequest}.
   * @param stream the stream to read from
   * @param adminRequest the {@link AdminRequest} that contains some necessary headers.
   * @return the {@link HealthCheckAdminRequest} constructed from the {@code stream}.
   * @throws IOException if there is any problem reading from the stream
   */
  public static HealthCheckAdminRequest readFrom(DataInputStream stream, AdminRequest adminRequest) throws IOException {
    Short versionId = stream.readShort();
    if (versionId != VERSION_1) {
      throw new IllegalStateException("Unrecognized version for HealthCheckAdminRequest: " + versionId);
    }
    return new HealthCheckAdminRequest(adminRequest);
  }

  /**
   * Constructor for {@link HealthCheckAdminRequest}.
   * @param adminRequest the {@link AdminRequest} that contains common admin request related information.
   */
  public HealthCheckAdminRequest(AdminRequest adminRequest) {
    super(AdminRequestOrResponseType.HealthCheck, adminRequest.getPartitionId(), adminRequest.getCorrelationId(),
        adminRequest.getClientId());
    sizeInBytes = super.sizeInBytes() + Short.BYTES;
  }

  @Override
  public long sizeInBytes() {
    return sizeInBytes;
  }

  @Override
  public String toString() {
    return "HealthCheckAdminRequest[ClientId=" + clientId + ", CorrelationId=" + correlationId + "]";
  }

  @Override
  protected void prepareBuffer() {
    super.prepareBuffer();
    bufferToSend.writeShort(VERSION_1);
  }
}
