package com.github.ambry.protocol;

import com.github.ambry.commons.ServerErrorCode;
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

  public static PutResponse readFrom(DataInputStream stream)
      throws IOException {
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
