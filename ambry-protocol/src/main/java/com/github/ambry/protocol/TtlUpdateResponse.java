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

import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;


/**
 * Response to a {@link TtlUpdateRequest}
 */
public class TtlUpdateResponse extends Response {
  private static final short TTL_UPDATE_RESPONSE_VERSION_V1 = 1;

  /**
   * @param correlationId the correlation id from the {@link TtlUpdateRequest}
   * @param clientId the id of the client from the {@link TtlUpdateRequest}
   * @param error the {@link ServerErrorCode} for the operation
   */
  public TtlUpdateResponse(int correlationId, String clientId, ServerErrorCode error) {
    super(RequestOrResponseType.TtlUpdateResponse, TTL_UPDATE_RESPONSE_VERSION_V1, correlationId, clientId, error);
  }

  /**
   * Helper to help construct TtlUpdateResponse from the {@code stream}.
   * @param stream the stream to read bytes from
   * @return a TtlUpdateResponse based on data read from the {@code stream}
   * @throws IOException if there was any problem reading the stream
   */
  public static TtlUpdateResponse readFrom(DataInputStream stream) throws IOException {
    RequestOrResponseType type = RequestOrResponseType.values()[stream.readShort()];
    if (type != RequestOrResponseType.TtlUpdateResponse) {
      throw new IllegalArgumentException("The type of request response is not compatible");
    }
    short version = stream.readShort();
    if (version != TTL_UPDATE_RESPONSE_VERSION_V1) {
      throw new IllegalStateException("Unknown TtlUpdateResponse version: " + version);
    }
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    ServerErrorCode error = ServerErrorCode.values()[stream.readShort()];
    return new TtlUpdateResponse(correlationId, clientId, error);
  }

  @Override
  public String toString() {
    return "TtlUpdateResponse[" + "ServerErrorCode=" + getError() + "]";
  }
}
