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

import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;


/**
 * Response of ReplicateBlob request
 */
public class ReplicateBlobResponse extends Response {
  private static final short Version_V1 = 1;

  public ReplicateBlobResponse(int correlationId, String clientId, ServerErrorCode error) {
    super(RequestOrResponseType.ReplicateBlobResponse, Version_V1, correlationId, clientId, error);
  }

  /**
   * Deserialize {@link ReplicateBlobResponse} from a given {@link DataInputStream}.
   * @param stream The stream that contains the serialized bytes.
   * @return A deserialized {@link ReplicateBlobResponse}.
   * @throws IOException Any I/O Errors.
   */
  public static ReplicateBlobResponse readFrom(DataInputStream stream) throws IOException {
    RequestOrResponseType type = RequestOrResponseType.values()[stream.readShort()];
    if (type != RequestOrResponseType.ReplicateBlobResponse) {
      throw new IllegalArgumentException("The type of request response is not compatible");
    }
    short versionId = stream.readShort();
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    ServerErrorCode error = ServerErrorCode.values()[stream.readShort()];
    if (versionId != Version_V1) {
      throw new IllegalArgumentException("Unexpected ReplicateBlobResponse version " + versionId);
    }
    return new ReplicateBlobResponse(correlationId, clientId, error);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("ReplicateBlobResponse[");
    sb.append("ServerErrorCode=").append(getError());
    sb.append("]");
    return sb.toString();
  }
}
