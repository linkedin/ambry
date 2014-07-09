package com.github.ambry.shared;

import com.github.ambry.network.Send;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;


/**
 * Request Response for serialization and de-serialization
 */
public abstract class RequestOrResponse implements Send {
  protected final RequestOrResponseType type;
  protected final int correlationId;
  protected short versionId;
  protected String clientId;
  protected ByteBuffer bufferToSend;
  protected Logger logger = LoggerFactory.getLogger(getClass());

  protected static final short Request_Response_Version = 1;

  private static final int Request_Response_Size_In_Bytes = 8;
  private static final int Request_Response_Type_Size_In_Bytes = 2;
  private static final int Request_Response_Version_Size_In_Bytes = 2;
  private static final int Correlation_Id_Size_In_Bytes = 4;
  private static final int ClientId_Field_Size_In_Bytes = 4;

  public RequestOrResponse(RequestOrResponseType type, short versionId, int correlationId, String clientId) {
    this.type = type;
    this.versionId = versionId;
    this.correlationId = correlationId;
    this.clientId = clientId;
    this.bufferToSend = null;
  }

  public RequestOrResponseType getRequestType() {
    return type;
  }

  public int getCorrelationId() {
    return correlationId;
  }

  public short getVersionId() {
    return versionId;
  }

  public String getClientId() {
    return clientId;
  }

  protected void writeHeader() {
    if (bufferToSend == null) {
      throw new IllegalStateException("Buffer to send should not be null");
    }
    bufferToSend.putLong(sizeInBytes());
    bufferToSend.putShort((short) type.ordinal());
    bufferToSend.putShort(versionId);
    bufferToSend.putInt(correlationId);
    bufferToSend.putInt(clientId.length());
    bufferToSend.put(clientId.getBytes());
  }

  public long sizeInBytes() {
    // size + type + versionId + correlationId + clientId
    return Request_Response_Size_In_Bytes + Request_Response_Type_Size_In_Bytes +
        Request_Response_Version_Size_In_Bytes + Correlation_Id_Size_In_Bytes +
        ClientId_Field_Size_In_Bytes + clientId.length();
  }
}


