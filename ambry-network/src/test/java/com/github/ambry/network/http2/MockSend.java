/**
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.network.http2;

import com.github.ambry.network.Send;
import com.github.ambry.utils.AbstractByteBufHolder;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;


/**
 * A mock implementation for {@link Send} that returns non-null value for {@link #content()} method.
 */
public class MockSend extends AbstractByteBufHolder<MockSend> implements Send {
  protected ByteBuf buf;

  MockSend(ByteBuf content) {
    buf = content;
  }

  @Override
  public long writeTo(WritableByteChannel channel) throws IOException {
    long written = channel.write(buf.nioBuffer());
    buf.skipBytes((int) written);
    return written;
  }

  @Override
  public boolean isSendComplete() {
    return buf.readableBytes() == 0;
  }

  @Override
  public long sizeInBytes() {
    return 16;
  }

  @Override
  public ByteBuf content() {
    return buf;
  }

  @Override
  public MockSend replace(ByteBuf content) {
    return null;
  }
}
