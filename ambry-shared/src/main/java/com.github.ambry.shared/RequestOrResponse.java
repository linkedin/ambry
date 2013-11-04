package com.github.ambry.shared;

import com.github.ambry.network.Send;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;


/**
 * Request Response for serialization and de-serialization
 */

public abstract class RequestOrResponse implements Send {
  protected final RequestResponseType type;
  protected final int correlationId;
  protected short versionId;
  protected ByteBuffer bufferToSend;
  protected Logger logger = LoggerFactory.getLogger(getClass());
  private static final int Request_Response_Size = 8;
  private static final int Request_Response_Type_Size = 2;
  private static final int Request_Response_Version_Size = 2;
  private static final int Correlation_Id_Size = 4;

  public RequestOrResponse(RequestResponseType type, short versionId, int correlationId) {
    this.type = type;
    this.versionId = versionId;
    this.correlationId = correlationId;
  }

  public RequestResponseType getRequestType() {
    return type;
  }

  public int getCorrelationId() {
    return correlationId;
  }

  public short getVersionId() {
    return versionId;
  }

  protected void writeHeader() {
    if (bufferToSend == null) {
      throw new IllegalStateException("Buffer to send should not be null");
    }
    bufferToSend.putLong(sizeInBytes());
    bufferToSend.putShort((short)type.ordinal());
    bufferToSend.putShort(versionId);
    bufferToSend.putInt(correlationId);
  }

  public long sizeInBytes() {
    // size + type + versionId + correlationId
    return Request_Response_Size + Request_Response_Type_Size +
            Request_Response_Version_Size + Correlation_Id_Size;
  }
}


