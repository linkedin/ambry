package com.github.ambry.protocol;

import com.github.ambry.server.ServerErrorCode;


public class StopCompactionResponse extends Response{
  public StopCompactionResponse(RequestOrResponseType type, short requestResponseVersion, int correlationId,
      String clientId, ServerErrorCode error) {
    super(type, requestResponseVersion, correlationId, clientId, error);
  }
}