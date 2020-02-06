/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
 * Representation of a response to an administration request.
 */
public class AdminResponse extends Response {
  private static final short ADMIN_RESPONSE_VERSION_V1 = 1;

  /**
   * Constructs a response.
   * @param correlationId an ID to help match responses to requests.
   * @param clientId the ID of the client.
   * @param error the {@link ServerErrorCode} for the request.
   */
  public AdminResponse(int correlationId, String clientId, ServerErrorCode error) {
    super(RequestOrResponseType.AdminResponse, ADMIN_RESPONSE_VERSION_V1, correlationId, clientId, error);
  }

  /**
   * Reads from a stream and constructs an {@link AdminResponse}.
   * @param stream  the {@link DataInputStream} to read from.
   * @return {@link AdminResponse} that is deserialized from the {@code stream}.
   * @throws IOException if there is an I/O error reading from {@code stream}
   */
  public static AdminResponse readFrom(DataInputStream stream) throws IOException {
    RequestOrResponseType type = RequestOrResponseType.values()[stream.readShort()];
    if (type != RequestOrResponseType.AdminResponse) {
      throw new IllegalArgumentException("The type of request response is not compatible (is " + type + ")");
    }
    Short versionId = stream.readShort();
    if (!versionId.equals(ADMIN_RESPONSE_VERSION_V1)) {
      throw new IllegalStateException("Unrecognized version for AdminResponse: " + versionId);
    }
    int correlationId = stream.readInt();
    String clientId = Utils.readIntString(stream);
    ServerErrorCode error = ServerErrorCode.values()[stream.readShort()];
    // ignore version for now
    return new AdminResponse(correlationId, clientId, error);
  }

  /**
   * A private method shared by {@link AdminResponse#writeTo(WritableByteChannel)} and
   * {@link AdminResponse#writeTo(AsyncWritableChannel, Callback)}.
   * This method allocate bufferToSend and write headers to it if bufferToSend is null.
   */
  private void prepareBufferToSend() {
    if (bufferToSend == null) {
      serializeIntoBuffer();
      bufferToSend.flip();
    }
  }

  @Override
  public long writeTo(WritableByteChannel channel) throws IOException {
    prepareBufferToSend();
    return bufferToSend.hasRemaining() ? channel.write(bufferToSend) : 0;
  }

  @Override
  public void writeTo(AsyncWritableChannel channel, Callback<Long> callback) {
    prepareBufferToSend();
    channel.write(bufferToSend, callback);
  }

  @Override
  public boolean isSendComplete() {
    return bufferToSend != null && bufferToSend.remaining() == 0;
  }

  /**
   * Serializes the response into bytes and loads into the buffer for sending.
   */
  protected void serializeIntoBuffer() {
    bufferToSend = ByteBuffer.allocate((int) sizeInBytes());
    writeHeader();
  }

  @Override
  public String toString() {
    return "AdminResponse[ClientId=" + clientId + ", CorrelationId=" + correlationId + ", Type=" + type
        + ", ServerError=" + getError() + "]";
  }
}
