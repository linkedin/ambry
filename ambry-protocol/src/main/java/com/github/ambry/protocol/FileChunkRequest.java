package com.github.ambry.protocol;

public class FileChunkRequest extends RequestOrResponse{
  public FileChunkRequest(RequestOrResponseType type, short versionId, int correlationId, String clientId) {
    super(type, versionId, correlationId, clientId);
  }
}
