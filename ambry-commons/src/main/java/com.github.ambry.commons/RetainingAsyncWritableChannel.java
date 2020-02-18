/*
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
 *
 */
package com.github.ambry.commons;

import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.router.AsyncWritableChannel;
import com.github.ambry.router.Callback;
import com.github.ambry.router.FutureResult;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;


/**
 * An implementation of {@link AsyncWritableChannel} that will retain a reference to any {@link ByteBuf}s passed to it.
 * It will assemble these buffers into a {@link CompositeByteBuf} that can then be consumed by the user of the channel.
 * Meant for interacting with blocking content consumers that would otherwise require a background thread if they did
 * not have all of the content available.
 */
public class RetainingAsyncWritableChannel implements AsyncWritableChannel {
  private final long sizeLimitInBytes;
  private volatile boolean open = true;
  private final AtomicLong totalBytesWritten = new AtomicLong(0);
  // accesses/updates to the buffer under construction must be protected by the lock
  private final Object bufferLock = new Object();
  private CompositeByteBuf compositeBuffer = ByteBufAllocator.DEFAULT.compositeBuffer();

  /**
   * Construct a {@link RetainingAsyncWritableChannel} with the size limit set to {@link Long#MAX_VALUE}.
   */
  public RetainingAsyncWritableChannel() {
    this(Integer.MAX_VALUE);
  }

  /**
   * @param sizeLimitInBytes the maximum number of bytes that can be written to this channel. If this limit is exceeded,
   *                         a {@link RestServiceException} will be provided to the write callback.
   */
  public RetainingAsyncWritableChannel(int sizeLimitInBytes) {
    this.sizeLimitInBytes = sizeLimitInBytes;
  }

  /**
   * @inheritDoc
   *
   * This method requires copying the buffer because {@link ByteBuffer}s do not have the concept of a reference count,
   * so the other side is free to reuse the buffer after the callback has been called (as opposed to when the ref count
   * reaches 0).
   */
  @Override
  public Future<Long> write(ByteBuffer src, Callback<Long> callback) {
    // still need to copy the buffer since the writer may decide to reuse the buffer after the callback is called.
    return writeInternal(() -> {
      ByteBuf copy = src.isDirect() ? ByteBufAllocator.DEFAULT.directBuffer(src.remaining())
          : PooledByteBufAllocator.DEFAULT.heapBuffer(src.remaining());
      copy.writeBytes(src);
      src.position(src.limit());
      return copy;
    }, callback);
  }

  @Override
  public Future<Long> write(ByteBuf src, Callback<Long> callback) {
    return writeInternal(() -> {
      ByteBuf duplicate = src.retainedDuplicate();
      // mark all bytes as read.
      src.skipBytes(src.readableBytes());
      return duplicate;
    }, callback);
  }

  /**
   * Internal method for writing to the channel that allows for a pluggable action to be taken to produce a
   * retained {@link ByteBuf} that can be added to the composite buffer.
   * @param retainedBufSupplier creates a retained {@link ByteBuf} that can be freely added to a
   *                            {@link CompositeByteBuf}.
   * @param callback called once the buffer has been added.
   * @return a future that is completed once the buffer has been added.
   */
  private Future<Long> writeInternal(Supplier<ByteBuf> retainedBufSupplier, Callback<Long> callback) {
    FutureResult<Long> future = new FutureResult<>();
    ByteBuf buf = null;
    long bytesWritten = 0;
    Exception exception = null;
    try {
      if (!isOpen()) {
        throw new ClosedChannelException();
      } else if (totalBytesWritten.get() > sizeLimitInBytes) {
        throw new RestServiceException("Request is larger than allowed size: " + sizeLimitInBytes,
            RestServiceErrorCode.RequestTooLarge);
      } else {
        synchronized (bufferLock) {
          if (compositeBuffer == null) {
            throw new IllegalStateException("Cannot write more data; content already consumed or channel was closed");
          }
          buf = retainedBufSupplier.get();
          bytesWritten = buf.readableBytes();
          compositeBuffer.addComponent(true, buf);
        }
        if (totalBytesWritten.addAndGet(bytesWritten) > sizeLimitInBytes) {
          exception = new RestServiceException("Request is larger than allowed size: " + sizeLimitInBytes,
              RestServiceErrorCode.RequestTooLarge);
        }
      }
    } catch (Exception e) {
      exception = e;
      if (buf != null) {
        buf.release();
      }
    } finally {
      future.done(bytesWritten, exception);
      if (callback != null) {
        callback.onCompletion(bytesWritten, exception);
      }
    }
    return future;
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public void close() {
    open = false;
    synchronized (bufferLock) {
      if (compositeBuffer != null) {
        compositeBuffer.release();
        compositeBuffer = null;
      }
    }
  }

  /**
   * Consume the collected content buffer. This method or {@link #consumeContentAsInputStream()} can only be called
   * once since ownership of the content is transferred to the caller.
   * @return a {@link ByteBuf} that contains the data from the chunks received up to this point in time.
   */
  public ByteBuf consumeContentAsByteBuf() {
    synchronized (bufferLock) {
      if (compositeBuffer == null) {
        throw new IllegalStateException("Content already consumed or channel was closed.");
      }
      try {
        return compositeBuffer.asReadOnly();
      } finally {
        compositeBuffer = null;
      }
    }
  }

  /**
   * @return an {@link InputStream} that contains the data from the chunks received up to this point in time.
   */
  public ByteBufInputStream consumeContentAsInputStream() {
    return new ByteBufInputStream(consumeContentAsByteBuf(), true);
  }

  /**
   * @return the number of bytes written to the channel at this point in time.
   */
  public long getBytesWritten() {
    return totalBytesWritten.get();
  }
}
