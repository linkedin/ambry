/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
 * A Response to the Put Request
 */
public class PutResponse extends Response {
  private static final short Put_Response_Version_V1 = 1;

  public PutResponse(int correlationId, String clientId, ServerErrorCode error) {
    super(RequestOrResponseType.PutResponse, Put_Response_Version_V1, correlationId, clientId, error);
  }

  public static PutResponse readFrom(DataInputStream stream) throws IOException {
    RequestOrResponseType type = RequestOrResponseType.values()[stream.readShort()];
    if (type != RequestOrResponseType.PutResponse) {
      throw new IllegalArgumentException("The type of request response is not compatible: " + type);
    }
    Short versionId = stream.readShort();
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    ServerErrorCode error = ServerErrorCode.values()[stream.readShort()];
    // ignore version for now
    return new PutResponse(correlationId, clientId, error);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("PutResponse[");
    sb.append("ServerErrorCode=").append(getError());
    sb.append("]");
    return sb.toString();
  }
}
