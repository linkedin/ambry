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

import com.github.ambry.router.AsyncWritableChannel;
import com.github.ambry.router.Callback;
import com.github.ambry.server.ServerErrorCode;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;


/**
 * Response for deserialization.
 */
public abstract class Response extends RequestOrResponse {
  private ServerErrorCode error;
  private static final int Error_Size_InBytes = 2;

  public Response(RequestOrResponseType type, short requestResponseVersion, int correlationId, String clientId,
      ServerErrorCode error) {
    super(type, requestResponseVersion, correlationId, clientId);
    this.error = error;
  }

  public ServerErrorCode getError() {
    return error;
  }

  @Override
  protected void writeHeader() {
    super.writeHeader();
    bufferToSend.putShort((short) error.ordinal());
  }

  private void prepareBuffer() {
    if (bufferToSend == null) {
      bufferToSend = ByteBuffer.allocate((int) sizeInBytes());
      writeHeader();
      bufferToSend.flip();
    }
  }

  @Override
  public long writeTo(WritableByteChannel channel) throws IOException {
    prepareBuffer();
    return bufferToSend.remaining() > 0 ? channel.write(bufferToSend) : 0;
  }

  @Override
  public void writeTo(AsyncWritableChannel channel, Callback<Long> callback) {
    prepareBuffer();
    channel.write(bufferToSend, callback);
  }

  @Override
  public boolean isSendComplete() {
    return (bufferToSend == null || bufferToSend.remaining() == 0);
  }

  @Override
  public long sizeInBytes() {
    // header + error
    return super.sizeInBytes() + Error_Size_InBytes;
  }
}
