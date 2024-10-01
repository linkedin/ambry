package com.github.ambry.protocol;

public class FileChunkRequest extends RequestOrResponse{

  String partitionName;
  final String fileName;
  long startOffSet;
  long sizeInBytes;


  public FileChunkRequest(RequestOrResponseType type, short versionId, int correlationId, String clientId,
      String fileName) {
    super(type, versionId, correlationId, clientId);
    this.fileName = fileName;
  }
}
