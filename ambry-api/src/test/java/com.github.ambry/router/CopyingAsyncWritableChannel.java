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

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.Future;


/**
 * An implementation of {@link AsyncWritableChannel} that copies the data it receives into a
 * {@link ByteArrayOutputStream}.
 * <p/>
 * This is not async and may be slow. Do not use if the test requires good performance.
 */
public class CopyingAsyncWritableChannel implements AsyncWritableChannel {
  private final ByteArrayOutputStream data;
  private volatile boolean channelOpen = true;

  public CopyingAsyncWritableChannel() {
    data = new ByteArrayOutputStream();
  }

  /**
   * Creates a CopyingAsyncWritableChannel with an initial size of {@code sizeHint}. It is ok if the actual data is
   * different from this size. The channel will expand as required.
   * @param sizeHint the initial space in the channel available for receiving data. Actual data size may differ from
   *                 this.
   */
  public CopyingAsyncWritableChannel(int sizeHint) {
    data = new ByteArrayOutputStream(sizeHint);
  }

  @Override
  public Future<Long> write(ByteBuffer src, Callback<Long> callback) {
    if (src == null) {
      throw new IllegalArgumentException("Source buffer cannot be null");
    }
    long bytesWritten = 0;
    Exception exception = null;
    FutureResult<Long> future = new FutureResult<Long>();
    if (!isOpen()) {
      exception = new ClosedChannelException();
    } else {
      byte[] srcData;
      int start = 0;
      int length = src.remaining();
      if (src.hasArray()) {
        srcData = src.array();
        start = src.arrayOffset();
      } else {
        srcData = new byte[length];
        src.get(srcData);
      }
      try {
        data.write(srcData, start, length);
        bytesWritten = length;
      } catch (Exception e) {
        exception = e;
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
    return channelOpen;
  }

  @Override
  public void close() {
    channelOpen = false;
  }

  /**
   * Gets the data that has been written to the channel. If the channel is still in use, this data may change between
   * invocations.
   * @return the data written to this channel.
   */
  public byte[] getData() {
    return data.toByteArray();
  }
}
