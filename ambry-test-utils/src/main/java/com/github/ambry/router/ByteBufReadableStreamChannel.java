/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.router;

import com.github.ambry.commons.Callback;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Represents a {@link ByteBuf} as a {@link ReadableStreamChannel}.
 */
public class ByteBufReadableStreamChannel implements ReadableStreamChannel {
  private final AtomicBoolean channelOpen = new AtomicBoolean(true);
  private final ByteBuf buf;
  private final int size;

  /**
   * Constructs a {@link ReadableStreamChannel} whose read operations return data from the provided {@code buf}.
   * @param buf the {@link ByteBuf} that is used to retrieve data from on invocation of read operations.
   */
  public ByteBufReadableStreamChannel(ByteBuf buf) {
    this.buf = buf;
    size = buf.readableBytes();
  }

  @Override
  public long getSize() {
    return size;
  }

  @Override
  public Future<Long> readInto(AsyncWritableChannel asyncWritableChannel, Callback<Long> callback) {
    Future<Long> future;
    if (!channelOpen.get()) {
      ClosedChannelException closedChannelException = new ClosedChannelException();
      FutureResult<Long> futureResult = new FutureResult<Long>();
      futureResult.done(0L, closedChannelException);
      future = futureResult;
      if (callback != null) {
        callback.onCompletion(0L, closedChannelException);
      }
    } else {
      future = asyncWritableChannel.write(buf, new Callback<Long>() {
        @Override
        public void onCompletion(Long result, Exception exception) {
          buf.release();
          if (callback != null) {
            callback.onCompletion(result, exception);
          }
        }
      });
    }
    return future;
  }

  @Override
  public boolean isOpen() {
    return channelOpen.get();
  }

  @Override
  public void close() throws IOException {
    channelOpen.set(false);
  }
}