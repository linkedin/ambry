package com.github.ambry.shared;

import com.github.ambry.network.Send;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;


/**
 * Created with IntelliJ IDEA.
 * User: srsubram
 * Date: 10/14/13
 * Time: 10:53 AM
 * To change this template use File | Settings | File Templates.
 */
enum RequestResponseType {
  PutRequest,
  GetRequest,
  PutReponse,
  GetResponse
}

public abstract class RequestOrResponse implements Send {
  protected final RequestResponseType type;
  protected final int correlationId;
  protected short versionId;
  protected ByteBuffer bufferToSend;
  protected Logger logger = LoggerFactory.getLogger(getClass());

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
    return 8 + 2 + 2 + 4;
  }
}


