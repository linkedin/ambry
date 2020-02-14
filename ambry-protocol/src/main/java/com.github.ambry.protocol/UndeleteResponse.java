/**
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.router.AsyncWritableChannel;
import com.github.ambry.router.Callback;
import com.github.ambry.server.ServerErrorCode;
import com.github.ambry.utils.Utils;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;


/**
 * Response of undelete rqeuest.
 */
public class UndeleteResponse extends Response {
  private static final short UNDELETE_RESPONSE_VERSION_1 = 1;
  public static final short INVALID_LIFE_VERSION = -1;

  private short lifeVersion;
  private static final int Life_Version_InBytes = Short.BYTES;

  /**
   * Constructs a {@link UndeleteResponse} with an {@link ServerErrorCode}.
   * @param correlationId correlationId of the undelete response.
   * @param clientId clientId of the undelete response.
   * @param error error code returned in this undelete response.
   */
  public UndeleteResponse(int correlationId, String clientId, ServerErrorCode error) {
    super(RequestOrResponseType.UndeleteResponse, UNDELETE_RESPONSE_VERSION_1, correlationId, clientId, error);
    if (error == ServerErrorCode.No_Error) {
      throw new IllegalArgumentException("NoError is not a valid error code");
    }
    this.lifeVersion = INVALID_LIFE_VERSION;
  }

  /**
   * Constructs a {@link UndeleteResponse} with a valid lifeVersion. The error code will be set to {@link ServerErrorCode#No_Error}.
   * @param correlationId correlationId of the undelete response.
   * @param clientId clientId of the undelete response.
   * @param lifeVersion a valid lifeVersion to return to client.
   */
  public UndeleteResponse(int correlationId, String clientId, short lifeVersion) {
    super(RequestOrResponseType.UndeleteResponse, UNDELETE_RESPONSE_VERSION_1, correlationId, clientId,
        ServerErrorCode.No_Error);
    if (lifeVersion == INVALID_LIFE_VERSION) {
      throw new IllegalArgumentException("Invalid life version " + lifeVersion);
    }
    this.lifeVersion = lifeVersion;
  }

  public static UndeleteResponse readFrom(DataInputStream stream) throws IOException {
    RequestOrResponseType type = RequestOrResponseType.values()[stream.readShort()];
    if (type != RequestOrResponseType.UndeleteResponse) {
      throw new IllegalArgumentException("The type of request response is not compatible");
    }
    Short versionId = stream.readShort();
    if (versionId != UNDELETE_RESPONSE_VERSION_1) {
      throw new IllegalArgumentException("Unknown version from stream " + versionId);
    }
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    ServerErrorCode error = ServerErrorCode.values()[stream.readShort()];
    if (error == ServerErrorCode.No_Error) {
      short lifeVersion = stream.readShort();
      return new UndeleteResponse(correlationId, clientId, lifeVersion);
    } else {
      return new UndeleteResponse(correlationId, clientId, error);
    }
  }

  private void prepareBuffer() {
    if (bufferToSend == null) {
      bufferToSend = ByteBuffer.allocate((int) sizeInBytes());
      writeHeader();
      if (getError() == ServerErrorCode.No_Error) {
        bufferToSend.putShort(lifeVersion);
      }
      bufferToSend.flip();
    }
  }

  @Override
  public long writeTo(WritableByteChannel channel) throws IOException {
    long written = 0;
    prepareBuffer();
    if (bufferToSend.remaining() > 0) {
      written = channel.write(bufferToSend);
    }
    return written;
  }

  @Override
  public void writeTo(AsyncWritableChannel channel, Callback<Long> callback) {
    prepareBuffer();
    channel.write(bufferToSend, callback);
  }

  @Override
  public long sizeInBytes() {
    return super.sizeInBytes() + (long) (getError() == ServerErrorCode.No_Error ? Life_Version_InBytes : 0);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("UndeleteResponse[");
    sb.append("ServerErrorCode=").append(getError());
    sb.append("LifeVersion=").append(lifeVersion);
    sb.append("]");
    return sb.toString();
  }

  /**
   * Return the lifeVersion.
   * @return the life version.
   */
  public short getLifeVersion() {
    return lifeVersion;
  }
}
