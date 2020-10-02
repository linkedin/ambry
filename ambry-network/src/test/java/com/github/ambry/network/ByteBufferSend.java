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

import com.github.ambry.utils.AbstractByteBufHolder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;


/**
 * A byte buffer version of Send that sends a materialized byte buffer. This breaks the contract of Send (only
 * materialize onto the network) and so is only suitable for use in tests.
 */
public class ByteBufferSend extends AbstractByteBufHolder<ByteBufferSend> implements Send {
  private final ByteBuffer buffer;

  public ByteBufferSend(ByteBuffer byteBuffer) {
    this.buffer = byteBuffer.duplicate();
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

  @Override
  public ByteBuf content() {
    return Unpooled.wrappedBuffer(buffer);
  }

  @Override
  public ByteBufferSend replace(ByteBuf content) {
    return null;
  }
}
