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
package com.github.ambry.network;

import com.github.ambry.utils.ByteBufferOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;


/**
 * A byte buffer version of Send to buffer the outgoing responses before sending.
 * This is mainly used to optimize serialization of response objects on the request handler
 * threads rather than the network threads.
 */
public class BoundedByteBufferSend implements Send {

  private final ByteBuffer buffer;

  public BoundedByteBufferSend(Send request) throws IOException {
    // Avoid possibility of overflow for 2GB-4 byte buffer
    if (request.sizeInBytes() > Integer.MAX_VALUE - 4) {
      throw new IllegalStateException(
          "Maximum allowable size for a bounded buffer is " + (Integer.MAX_VALUE - 4) + ".");
    }

    int size = (int) request.sizeInBytes();
    buffer = ByteBuffer.allocate(size + 4);
    buffer.putInt(size);
    WritableByteChannel channel = Channels.newChannel(new ByteBufferOutputStream(buffer));
    request.writeTo(channel);
    buffer.rewind();
  }

  public BoundedByteBufferSend(ByteBuffer buffer) {
    if (buffer == null) {
      throw new IllegalArgumentException("Input buffer cannot be null for BoundedByteBufferSend");
    }
    this.buffer = buffer;
  }

  @Override
  public long writeTo(WritableByteChannel channel) throws IOException {
    return isSendComplete() ? 0 : channel.write(buffer);
  }

  @Override
  public boolean isSendComplete() {
    return buffer.remaining() == 0;
  }

  @Override
  public long sizeInBytes() {
    return buffer.limit();
  }

  public BoundedByteBufferSend duplicate() {
    return new BoundedByteBufferSend(buffer.duplicate());
  }
}