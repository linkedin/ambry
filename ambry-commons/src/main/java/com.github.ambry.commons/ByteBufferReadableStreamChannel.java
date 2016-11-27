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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Represents a {@link ByteBuffer} as a {@link ReadableStreamChannel}.
 */
public class ByteBufferReadableStreamChannel implements ReadableStreamChannel {
  private final AtomicBoolean channelOpen = new AtomicBoolean(true);
  private final AtomicBoolean channelEmptied = new AtomicBoolean(false);
  private final List<ByteBuffer> buffers;
  private final int size;

  /**
   * Constructs a {@link ReadableStreamChannel} whose read operations return data from the provided {@code buffer}.
   * @param buffer the {@link ByteBuffer} that is used to retrieve data from on invocation of read operations.
   */
  public ByteBufferReadableStreamChannel(ByteBuffer buffer) {
    this(Arrays.asList(buffer));
  }

  /**
   * Constructs a {@link ReadableStreamChannel} whose read operations return data from the provided {@code bufferList}.
   * @param bufferList list of {@link ByteBuffer} that is used to retrieve data from on invocation of read operations.
   */
  public ByteBufferReadableStreamChannel(List<ByteBuffer> bufferList) {
    if (bufferList == null || bufferList.isEmpty()) {
      throw new IllegalArgumentException(
          "Input List of ByteBuffer cannot be null/empty for ByteBufferReadableStreamChannel");
    }
    buffers = bufferList;
    size = computeSize(bufferList);
  }

  @Override
  public long getSize() {
    return size;
  }

  @Override
  public Future<Long> readInto(AsyncWritableChannel asyncWritableChannel, Callback<Long> callback) {
    ReadIntoCallbackWrapper callbackWrapper = new ReadIntoCallbackWrapper(callback);
    if (!channelOpen.get()) {
      callbackWrapper.invokeCallback(new ClosedChannelException());
    } else if (!channelEmptied.compareAndSet(false, true)) {
      throw new IllegalStateException("ReadableStreamChannel cannot be read more than once");
    }
    int bufferListSize = buffers.size();
    for (int i = 0; i < bufferListSize; i++) {
      writeContent(asyncWritableChannel, callbackWrapper, buffers.get(i), i + 1 == bufferListSize);
    }
    return callbackWrapper.futureResult;
  }

  /**
   * Writes the provided {@code content} to the given {@code writeChannel}.
   * @param writeChannel the {@link AsyncWritableChannel} to write the {@code content} to.
   * @param callbackWrapper the {@link ReadIntoCallbackWrapper} for the read operation.
   * @param content the piece of {@link ByteBuffer} that needs to be written to the {@code writeChannel}.
   * @param isLast if this the last piece of content in {@code buffers}.
   */
  private void writeContent(AsyncWritableChannel writeChannel, ReadIntoCallbackWrapper callbackWrapper,
      ByteBuffer content, boolean isLast) {
    ContentWriteCallback writeCallback = new ContentWriteCallback(isLast, callbackWrapper);
    writeChannel.write(content, writeCallback);
  }

  @Override
  public boolean isOpen() {
    return channelOpen.get();
  }

  @Override
  public void close() throws IOException {
    channelOpen.set(false);
  }

  /**
   * Computes the total size of the given {@code bufferList}
   * @param bufferList list of {@link ByteBuffer} that is used to retrieve data from on invocation of read operations.
   */
  private int computeSize(List<ByteBuffer> bufferList) {
    int totalSize = 0;
    for (ByteBuffer buffer : bufferList) {
      totalSize += buffer.remaining();
    }
    return totalSize;
  }

  /**
   * Callback for each write into the given {@link AsyncWritableChannel}.
   */
  private class ContentWriteCallback implements Callback<Long> {
    private final boolean isLast;
    private final ReadIntoCallbackWrapper callbackWrapper;

    /**
     * Creates a new instance of ContentWriteCallback.
     * @param isLast if this is the last piece of content for this request.
     * @param callbackWrapper the {@link ReadIntoCallbackWrapper} that will receive updates of bytes read and one that
     *                        should be invoked in {@link #onCompletion(Long, Exception)} if {@code isLast} is
     *                        {@code true} or exception passed is not null.
     */
    public ContentWriteCallback(boolean isLast, ReadIntoCallbackWrapper callbackWrapper) {
      this.isLast = isLast;
      this.callbackWrapper = callbackWrapper;
    }

    /**
     * Updates the number of bytes read and invokes {@link ReadIntoCallbackWrapper#invokeCallback(Exception)} if
     * {@code exception} is not {@code null} or if this is the last piece of content in the request.
     * @param result The result of the request. This would be non null when the request executed successfully
     * @param exception The exception that was reported on execution of the request
     */
    @Override
    public void onCompletion(Long result, Exception exception) {
      callbackWrapper.updateBytesRead(result);
      if (exception != null || isLast) {
        callbackWrapper.invokeCallback(exception);
      }
    }
  }

  /**
   * Wrapper for callbacks provided to {@link ByteBufferReadableStreamChannel#readInto(AsyncWritableChannel, Callback)}.
   */
  private class ReadIntoCallbackWrapper {
    /**
     * The {@link Future} where the result of {@link ByteBufferReadableStreamChannel#readInto(AsyncWritableChannel, Callback)} will
     * eventually be updated.
     */
    public final FutureResult<Long> futureResult = new FutureResult<>();

    private final Callback<Long> callback;
    private final AtomicLong totalBytesRead = new AtomicLong(0);
    private final AtomicBoolean callbackInvoked = new AtomicBoolean(false);

    /**
     * Creates an instance of ReadIntoCallbackWrapper with the given {@code callback}.
     * @param callback the {@link Callback} to invoke on operation completion.
     */
    public ReadIntoCallbackWrapper(Callback<Long> callback) {
      this.callback = callback;
    }

    /**
     * Updates the number of bytes that have been successfully read into the given {@link AsyncWritableChannel}.
     * @param delta the number of bytes read in the current invocation.
     * @return the total number of bytes read until now.
     */
    public long updateBytesRead(long delta) {
      return totalBytesRead.addAndGet(delta);
    }

    /**
     * Invokes the callback and updates the future once this is called. This function ensures that the callback is invoked
     * just once.
     * @param exception the {@link Exception}, if any, to pass to the callback.
     */
    public void invokeCallback(Exception exception) {
      if (callbackInvoked.compareAndSet(false, true)) {
        futureResult.done(totalBytesRead.get(), exception);
        if (callback != null) {
          callback.onCompletion(totalBytesRead.get(), exception);
        }
      }
    }
  }
}
