package com.github.ambry.protocol;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ServerCnxn;


public class FileMetaDataRequest extends RequestOrResponse {

  public FileMetaDataRequest(RequestOrResponseType type, short versionId, int correlationId, String clientId) {
    super(type, versionId, correlationId, clientId);
  }


}
