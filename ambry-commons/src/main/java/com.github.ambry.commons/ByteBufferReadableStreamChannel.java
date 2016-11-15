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
package com.github.ambry.commons;

import com.github.ambry.router.AsyncWritableChannel;
import com.github.ambry.router.Callback;
import com.github.ambry.router.FutureResult;
import com.github.ambry.router.ReadableStreamChannel;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Represents a {@link ByteBuffer} as a {@link ReadableStreamChannel}.
 */
public class ByteBufferReadableStreamChannel implements ReadableStreamChannel {
  private final AtomicBoolean channelOpen = new AtomicBoolean(true);
  private final AtomicBoolean channelEmptied = new AtomicBoolean(false);
  private final ByteBuffer buffer;
  private final int size;

  /**
   * Constructs a {@link ReadableStreamChannel} whose read operations return data from the provided {@code buffer}.
   * @param buffer the {@link ByteBuffer} that is used to retrieve data from on invocation of read operations.
   */
  public ByteBufferReadableStreamChannel(ByteBuffer buffer) {
    this.buffer = buffer;
    size = buffer.remaining();
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
    } else if (!channelEmptied.compareAndSet(false, true)) {
      throw new IllegalStateException("ReadableStreamChannel cannot be read more than once");
    } else {
      future = asyncWritableChannel.write(buffer, callback);
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
