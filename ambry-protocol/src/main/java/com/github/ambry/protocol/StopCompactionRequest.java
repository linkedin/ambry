package com.github.ambry.protocol;

public class StopCompactionRequest extends RequestOrResponse{
  public StopCompactionRequest(RequestOrResponseType type, short versionId, int correlationId, String clientId) {
    super(type, versionId, correlationId, clientId);
  }
}
