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
package com.github.ambry.router;

import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;


/**
 * A channel that is used to perform writes asynchronously. The channel guarantees that all the data submitted to it
 * will either be acknowledged as successfully written or the reason for failure will be notified.
 * <p/>
 * The ordering in which data is submitted to the channel (even across multiple threads) is the order in which data
 * will be written to any underlying I/O channels or data structures.
 */
public interface AsyncWritableChannel extends Channel {

  /**
   * The data in {@code src} will be eventually written to the channel and the {@code callback} will be invoked once the
   * write succeeds/fails. The {@code callback} and the future returned will contain the bytes written
   * (that should be equal to {@code src.remaining()} at the time of invocation if there were no exceptions) on success
   * or failure. If the write failed, they will also contain the exception that caused the failure.
   * <p/>
   * Every single write is guaranteed to be acknowledged as either succeeded or failed even if the channel is closed.
   * Further, writes will be acknowledged in the same order that they were received.
   * <p/>
   * {@code src} can be reused only after the {@code callback} is invoked (or after {@code future.get()} returns).
   * <p/>
   * Concurrent write calls may result in unexpected behavior.
   * @param src the data that needs to be written to the channel.
   * @param callback the {@link Callback} that will be invoked once the write succeeds/fails. This can be null.
   * @return a {@link Future} that will eventually contain the result of the write operation (the number of bytes
   *         written).
   */
  Future<Long> write(ByteBuffer src, Callback<Long> callback);

  /**
   * Write data in {@code src} to the channel and the {@code callback} will be invoked once the write succeeds or fails.
   * This method is the counterpart for {@link ByteBuf}. It shares the same guarantee as the {@link #write(ByteBuffer, Callback)}.
   * Whoever implements this interface, shouldn't release the {@code src}. If releasing is expected after finishing writing
   * to channel, please release this {@link ByteBuf} in the callback method.
   * @param src The data taht needs to be written to the channel.
   * @param callback The {@link Callback} that will be invoked once the write succeeds/fails. This can be null.
   * @return a {@link Future} that will eventually contain the result of the write operation (the number of bytes
   *         written).
   */
  default Future<Long> write(ByteBuf src, Callback<Long> callback) {
    if (src == null) {
      throw new IllegalArgumentException("Source ByteBuf cannot be null");
    }
    int numBuffers = src.nioBufferCount();
    FutureResult<Long> futureResult = new FutureResult<>();
    Callback<Long> singleBufferCallback = (result, exception) -> {
      if (result != 0) {
        src.readerIndex(src.readerIndex() + (int) result.longValue());
      }
      futureResult.done(result, exception);
      if (callback != null) {
        callback.onCompletion(result, exception);
      }
    };
    if (numBuffers < 1) {
      ByteBuffer byteBuffer = ByteBuffer.allocate(src.readableBytes());
      src.getBytes(src.readerIndex(), byteBuffer);
      write(byteBuffer, singleBufferCallback);
    } else if (numBuffers == 1) {
      write(src.nioBuffer(), singleBufferCallback);
    } else {
      ByteBuffer[] buffers = src.nioBuffers();
      AtomicLong size = new AtomicLong(0);
      AtomicInteger index = new AtomicInteger(0);
      AtomicBoolean callbackInvoked = new AtomicBoolean(false);
      Callback<Long> cb = (result, exception) -> {
        index.addAndGet(1);
        size.addAndGet(result);
        if (result != 0) {
          src.readerIndex(src.readerIndex() + (int) result.longValue());
        }
        if ((exception != null || index.get() == buffers.length) && callbackInvoked.compareAndSet(false, true)) {
          futureResult.done(size.get(), exception);
          if (callback != null) {
            callback.onCompletion(size.get(), exception);
          }
        }
      };
      for (int i = 0; i < buffers.length; i++) {
        write(buffers[i], cb);
      }
    }
    return futureResult;
  }
}
