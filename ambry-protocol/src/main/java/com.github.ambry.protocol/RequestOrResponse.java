/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.protocol;

import com.github.ambry.network.SendWithCorrelationId;
import com.github.ambry.utils.Utils;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Request Response for serialization and de-serialization
 */
public abstract class RequestOrResponse implements SendWithCorrelationId {
  protected final RequestOrResponseType type;
  protected final int correlationId;
  protected short versionId;
  protected String clientId;
  protected ByteBuffer bufferToSend;
  protected Logger logger = LoggerFactory.getLogger(getClass());

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

  @Override
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
    Utils.serializeString(bufferToSend, clientId, Charset.defaultCharset());
  }

  public long sizeInBytes() {
    // size + type + versionId + correlationId + clientId
    return Request_Response_Size_In_Bytes + Request_Response_Type_Size_In_Bytes + Request_Response_Version_Size_In_Bytes
        + Correlation_Id_Size_In_Bytes + ClientId_Field_Size_In_Bytes + clientId.length();
  }
}


