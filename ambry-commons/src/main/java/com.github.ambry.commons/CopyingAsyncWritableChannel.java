/*
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;


/**
 * An implementation of {@link AsyncWritableChannel} that will copy all received chunks into a sequence of byte arrays.
 * Meant for interacting with blocking content consumers that would otherwise require a background thread.
 */
public class CopyingAsyncWritableChannel implements AsyncWritableChannel {
  private final long sizeLimitInBytes;
  private final AtomicLong totalBytesWritten = new AtomicLong(0);
  private final Queue<byte[]> outputBuffers = new ConcurrentLinkedQueue<>();
  private volatile boolean open = true;

  /**
   * Construct a {@link CopyingAsyncWritableChannel} with the size limit set to {@link Long#MAX_VALUE}.
   */
  public CopyingAsyncWritableChannel() {
    this(Long.MAX_VALUE);
  }
  /**
   * @param sizeLimitInBytes the maximum number of bytes that can be written to this channel. If this limit is exceeded,
   *                         a {@link RestServiceException} will be provided to the write callback.
   */
  public CopyingAsyncWritableChannel(long sizeLimitInBytes) {
    this.sizeLimitInBytes = sizeLimitInBytes;
  }

  @Override
  public Future<Long> write(ByteBuffer src, Callback<Long> callback) {
    FutureResult<Long> future = new FutureResult<>();
    long bytesWritten = 0;
    Exception exception = null;
    if (!isOpen()) {
      exception = new ClosedChannelException();
    } else if (totalBytesWritten.get() > sizeLimitInBytes) {
      exception = new RestServiceException("Request is larger than allowed size: " + sizeLimitInBytes,
          RestServiceErrorCode.RequestTooLarge);
    } else {
      byte[] chunkCopy = new byte[src.remaining()];
      src.get(chunkCopy);
      outputBuffers.add(chunkCopy);
      bytesWritten = chunkCopy.length;
      if (totalBytesWritten.addAndGet(bytesWritten) > sizeLimitInBytes) {
        exception = new RestServiceException("Request is larger than allowed size: " + sizeLimitInBytes,
            RestServiceErrorCode.RequestTooLarge);
      }
    }
    future.done(bytesWritten, exception);
    if (callback != null) {
      callback.onCompletion(bytesWritten, exception);
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
  }

  /**
   * @return an {@link InputStream} that contains the data from the chunks received up to this point in time.
   */
  public InputStream getContentAsInputStream() {
    return new SequenceInputStream(
        iteratorToEnumeration(outputBuffers.stream().map(ByteArrayInputStream::new).iterator()));
  }

  /**
   * @return the number of bytes written to the channel at this point in time.
   */
  public long getBytesWritten() {
    return totalBytesWritten.get();
  }

  /**
   * Convert an {@link Iterator} into an {@link Enumeration}.
   * @param iterator the {@link Iterator}
   * @return the {@link Enumeration}
   */
  private static <T> Enumeration<T> iteratorToEnumeration(Iterator<? extends T> iterator) {
    return new Enumeration<T>() {
      @Override
      public boolean hasMoreElements() {
        return iterator.hasNext();
      }

      @Override
      public T nextElement() {
        return iterator.next();
      }
    };
  }
}
