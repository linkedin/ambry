package com.github.ambry.shared;

import com.github.ambry.utils.Utils;

import java.io.DataInputStream;
import java.io.IOException;

// TODO: Rename to CancelTTLResponse

/**
 * Response of TTL request
 */
public class TTLResponse extends Response {

  public TTLResponse(int correlationId, String clientId, ServerErrorCode error) {
    super(RequestResponseType.TTLResponse, Request_Response_Version, correlationId, clientId, error);
  }

  public static TTLResponse readFrom(DataInputStream stream) throws IOException {
    RequestResponseType type = RequestResponseType.values()[stream.readShort()];
    if (type != RequestResponseType.TTLResponse) {
      throw new IllegalArgumentException("The type of request response is not compatible");
    }
    Short versionId  = stream.readShort();
    // ignore version for now
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    ServerErrorCode error = ServerErrorCode.values()[stream.readShort()];
    return new TTLResponse(correlationId, clientId, error);
  }

}
