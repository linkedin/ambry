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

import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;


/**
 * Response to a {@link PurgeRequest}.
 * The main purpose of the response is to indicate whether the purge request succeeded or not.
 */
public class PurgeResponse extends Response {
  private static final short PURGE_RESPONSE_VERSION_1 = 1;

  /**
   * @param correlationId the correlation id from the {@link PurgeRequest}
   * @param clientId the id of the client from the {@link PurgeRequest}
   * @param error the {@link ServerErrorCode} for the operation
   */
  public PurgeResponse(int correlationId, String clientId, ServerErrorCode error) {
    super(RequestOrResponseType.PurgeResponse, PURGE_RESPONSE_VERSION_1, correlationId, clientId, error);
  }

  /**
   * Helper to help construct {@link PurgeResponse} from the {@code stream}.
   * @param stream the stream to read bytes from
   * @return a {@link PurgeResponse} based on data read from the {@code stream}
   * @throws IOException if there was any problem reading the stream
   */
  public static PurgeResponse readFrom(DataInputStream stream) throws IOException {
    RequestOrResponseType type = RequestOrResponseType.values()[stream.readShort()];
    if (type != RequestOrResponseType.PurgeResponse) {
      throw new IllegalArgumentException("The type of request response is not compatible");
    }
    short version = stream.readShort();
    if (version != PURGE_RESPONSE_VERSION_1) {
      throw new IllegalStateException("Unknown PurgeResponse version: " + version);
    }
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    ServerErrorCode error = ServerErrorCode.values()[stream.readShort()];
    return new PurgeResponse(correlationId, clientId, error);
  }

  @Override
  public String toString() {
    return "PurgeResponse[" + "ServerErrorCode=" + getError() + "]";
  }
}
