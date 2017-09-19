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
 * An admin request used to control request behavior (enable/disable)
 */
public class RequestControlAdminRequest extends AdminRequest {
  private static final short VERSION_V1 = 1;

  private final RequestOrResponseType requestTypeToControl;
  private final boolean enable;
  private final long sizeInBytes;

  /**
   * Reads from a stream and constructs a {@link RequestControlAdminRequest}.
   * @param stream the stream to read from
   * @param adminRequest the {@link AdminRequest} that contains some necessary headers.
   * @return the {@link RequestControlAdminRequest} constructed from the {@code stream}.
   * @throws IOException
   */
  public static RequestControlAdminRequest readFrom(DataInputStream stream, AdminRequest adminRequest)
      throws IOException {
    Short versionId = stream.readShort();
    if (!versionId.equals(VERSION_V1)) {
      throw new IllegalStateException("Unrecognized version for RequestControlAdminRequest: " + versionId);
    }
    RequestOrResponseType requestType = RequestOrResponseType.values()[stream.readShort()];
    boolean enable = stream.readByte() == 1;
    return new RequestControlAdminRequest(requestType, enable, adminRequest);
  }

  /**
   * Construct a RequestControlAdminRequest
   * @param requestTypeToControl the {@link RequestOrResponseType} to control.
   * @param enable enable/disable flag ({@code true} to enable).
   * @param adminRequest the {@link AdminRequest} that contains common admin request related information.
   */
  public RequestControlAdminRequest(RequestOrResponseType requestTypeToControl, boolean enable,
      AdminRequest adminRequest) {
    super(AdminRequestOrResponseType.RequestControl, adminRequest.getPartitionId(), adminRequest.getCorrelationId(),
        adminRequest.getClientId());
    this.requestTypeToControl = requestTypeToControl;
    this.enable = enable;
    // parent size + version size + request type size + enable flag size
    sizeInBytes = super.sizeInBytes() + Short.BYTES + Short.BYTES + Byte.BYTES;
  }

  /**
   * @return the {@link RequestOrResponseType} to control.
   */
  public RequestOrResponseType getRequestTypeToControl() {
    return requestTypeToControl;
  }

  /**
   * @return if {@link #getRequestTypeToControl()} needs to enabled ({@code true}) or disabled ({@code false}).
   */
  public boolean shouldEnable() {
    return enable;
  }

  @Override
  public long sizeInBytes() {
    return sizeInBytes;
  }

  @Override
  public String toString() {
    return "RequestControlAdminRequest[ClientId=" + clientId + ", CorrelationId=" + correlationId
        + ", RequestTypeToControl=" + requestTypeToControl + ", PartitionId=" + getPartitionId() + "]";
  }

  @Override
  protected void serializeIntoBuffer() {
    super.serializeIntoBuffer();
    bufferToSend.putShort(VERSION_V1);
    bufferToSend.putShort((short) requestTypeToControl.ordinal());
    bufferToSend.put(enable ? (byte) 1 : 0);
  }
}
