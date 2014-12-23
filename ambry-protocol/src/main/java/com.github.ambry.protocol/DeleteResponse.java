package com.github.ambry.protocol;

import com.github.ambry.commons.ServerErrorCode;
import com.github.ambry.utils.Utils;

import java.io.DataInputStream;
import java.io.IOException;


/**
 * Response of delete request
 */
public class DeleteResponse extends Response {

  public DeleteResponse(int correlationId, String clientId, ServerErrorCode error) {
    super(RequestOrResponseType.DeleteResponse, Request_Response_Version, correlationId, clientId, error);
  }

  public static DeleteResponse readFrom(DataInputStream stream)
      throws IOException {
    RequestOrResponseType type = RequestOrResponseType.values()[stream.readShort()];
    if (type != RequestOrResponseType.DeleteResponse) {
      throw new IllegalArgumentException("The type of request response is not compatible");
    }
    Short versionId = stream.readShort();
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    ServerErrorCode error = ServerErrorCode.values()[stream.readShort()];
    // ignore version for now
    return new DeleteResponse(correlationId, clientId, error);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("DeleteResponse[");
    sb.append("ServerErrorCode=").append(getError());
    sb.append("]");
    return sb.toString();
  }
}
