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
package com.github.ambry.tools.perf.rest;

import com.github.ambry.router.AsyncWritableChannel;
import com.github.ambry.router.Callback;
import com.github.ambry.router.FutureResult;
import com.github.ambry.router.ReadableStreamChannel;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * An implementation of {@link ReadableStreamChannel} that uses a byte array repeatedly to construct a stream of the
 * specified size.
 */
class PerfRSC implements ReadableStreamChannel {
  private final ByteBuffer bytes;
  private final long streamSize;
  private final AtomicBoolean readIntoInvoked = new AtomicBoolean(false);
  private final AtomicBoolean callbackInvoked = new AtomicBoolean(false);
  private final AtomicBoolean isOpen = new AtomicBoolean(true);

  private volatile long streamed = 0;
  private volatile Exception channelWriteException = null;

  private volatile AsyncWritableChannel writeChannel = null;
  private volatile FutureResult<Long> futureResult = new FutureResult<Long>();
  private volatile Callback<Long> readIntoCallback = null;

  /**
   * Create an instance of PerfRSC.
   * @param bytes the bytes that form a chunk that will be repeatedly returned until {@code streamSize} is reached.
   * @param streamSize the desired total size of the stream.
   */
  public PerfRSC(byte[] bytes, long streamSize) {
    if (streamSize < 0 || (streamSize > 0 && bytes.length < 1)) {
      throw new IllegalArgumentException("Invalid argument(s)");
    }
    this.streamSize = streamSize;
    if (bytes != null) {
      this.bytes = ByteBuffer.wrap(bytes);
    } else {
      this.bytes = null;
    }
  }

  @Override
  public long getSize() {
    return streamSize;
  }

  @Override
  public Future<Long> readInto(AsyncWritableChannel asyncWritableChannel, Callback<Long> callback) {
    if (readIntoInvoked.compareAndSet(false, true)) {
      writeChannel = asyncWritableChannel;
      readIntoCallback = callback;
      writeToChannel();
    } else {
      throw new IllegalStateException("ReadInto should not be called more than once");
    }
    return futureResult;
  }

  @Override
  public boolean isOpen() {
    return isOpen.get();
  }

  @Override
  public void close() {
    isOpen.set(false);
  }

  /**
   * Writes continuously to the provided {@link AsyncWritableChannel} until {@link #streamSize} is reached.
   */
  private void writeToChannel() {
    if (channelWriteException == null && streamed < streamSize) {
      if (!isOpen()) {
        invokeCallback(streamed, new ClosedChannelException());
      } else {
        int bytesLeft = streamSize - streamed > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) (streamSize - streamed);
        bytes.rewind();
        bytes.limit(Math.min(bytes.capacity(), bytesLeft));
        writeChannel.write(bytes, new Callback<Long>() {
          @Override
          public void onCompletion(Long result, Exception callbackException) {
            streamed += result;
            channelWriteException = callbackException;
            writeToChannel();
          }
        });
      }
    } else {
      invokeCallback(streamed, channelWriteException);
    }
  }

  /**
   * Invokes the callback and updates the future once this is called. This function ensures that the callback is invoked
   * just once.
   * @param bytesRead the number of bytes read.
   * @param exception the {@link Exception}, if any, to pass to the callback.
   */
  private void invokeCallback(long bytesRead, Exception exception) {
    if (callbackInvoked.compareAndSet(false, true)) {
      futureResult.done(bytesRead, exception);
      if (readIntoCallback != null) {
        readIntoCallback.onCompletion(bytesRead, exception);
      }
    }
  }
}
