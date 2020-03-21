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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Represents a {@link InputStream} as a {@link ReadableStreamChannel}.
 */
public class InputStreamReadableStreamChannel implements ReadableStreamChannel {
  static final int BUFFER_SIZE = 8192;

  private final AtomicBoolean channelOpen = new AtomicBoolean(true);
  private final AtomicBoolean readIntoCalled = new AtomicBoolean(false);
  private final InputStream inputStream;
  private final long size;
  private final ExecutorService executorService;
  private final byte[] buffer;

  private ReadIntoCallbackWrapper callbackWrapper = null;
  private ByteBuffer bufferToWrite;

  /**
   * Constructs a {@link ReadableStreamChannel} whose read operations return data from the provided {@code inputStream}.
   * @param inputStream the {@link InputStream} that is used to retrieve data from - on invocation of read operations.
   * @param executorService the {@link ExecutorService} that will be used to schedule blocking reads from the
   *                        {@code inputStream}.
   */
  public InputStreamReadableStreamChannel(InputStream inputStream, ExecutorService executorService) {
    this(inputStream, -1, executorService);
  }

  /**
   * Constructs a {@link ReadableStreamChannel} whose read operations return data from the provided {@code inputStream}.
   * <p/>
   * Note that {@code size} does not refer the number of bytes that is read from the {@code inputStream}. It refers
   * to the total size of the stream and is only used to return the size of the channel on {@link #getSize()}. The
   * {@code inputStream} is always read completely if {@code size} > 0.
   * @param inputStream the {@link InputStream} that is used to retrieve data from - on invocation of read operations.
   * @param size the total size of the {@code inputStream}. -1 if unknown.
   * @param executorService the {@link ExecutorService} that will be used to schedule blocking reads from the
   *                        {@code inputStream}.
   */
  public InputStreamReadableStreamChannel(InputStream inputStream, long size, ExecutorService executorService) {
    this.inputStream = inputStream;
    this.size = size;
    this.executorService = executorService;
    int sizeToAllocate = BUFFER_SIZE;
    if (size >= 0 && size < BUFFER_SIZE) {
      sizeToAllocate = (int) size;
    }
    buffer = new byte[sizeToAllocate];
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
    } else if (!readIntoCalled.compareAndSet(false, true)) {
      throw new IllegalStateException("ReadableStreamChannel cannot be read more than once");
    } else {
      callbackWrapper = new ReadIntoCallbackWrapper(callback);
      future = callbackWrapper.futureResult;
      if (size == 0) {
        callbackWrapper.invokeCallback(null);
      } else {
        writeToChannel(asyncWritableChannel);
      }
    }
    return future;
  }

  @Override
  public boolean isOpen() {
    return channelOpen.get();
  }

  /**
   * {@inheritDoc}
   * <p/>
   * Also closes the underlying {@link InputStream}.
   * @throws IOException
   */
  @Override
  public void close() throws IOException {
    if (channelOpen.compareAndSet(true, false)) {
      inputStream.close();
    }
  }

  /**
   * Reads data from the {@link InputStream} asynchronously and writes it to the given {@code writableChannel}
   * @param writableChannel the {@link AsyncWritableChannel} into which data from the {@link InputStream}is written.
   */
  private void writeToChannel(final AsyncWritableChannel writableChannel) {
    executorService.submit(new Runnable() {
      @Override
      public void run() {
        try {
          final int read = fillBuffer();
          if (read > 0) {
            bufferToWrite = ByteBuffer.wrap(buffer);
            bufferToWrite.limit(read);
            writableChannel.write(bufferToWrite, new Callback<Long>() {
              @Override
              public void onCompletion(Long result, Exception exception) {
                if (exception != null) {
                  callbackWrapper.invokeCallback(exception);
                } else {
                  callbackWrapper.updateBytesRead(result);
                  writeToChannel(writableChannel);
                }
              }
            });
          } else {
            // the input stream has been emptied.
            callbackWrapper.invokeCallback(null);
          }
        } catch (Exception e) {
          callbackWrapper.invokeCallback(e);
        }
      }
    });
  }

  /**
   * Fills the buffer from the {@link InputStream}. Waits untils the buffer is full or the {@link InputStream} is empty.
   * @return the number of bytes read from the {@link InputStream}.
   * @throws IOException if there were any problems reading the {@link InputStream}.
   */
  private int fillBuffer() throws IOException {
    int totalRead = 0;
    int currentRead;
    do {
      currentRead = inputStream.read(buffer, totalRead, buffer.length - totalRead);
      if (currentRead > 0) {
        totalRead += currentRead;
      }
    } while (totalRead < buffer.length && currentRead != -1);
    return totalRead;
  }

  /**
   * Wrapper for callbacks provided to {@link #readInto(AsyncWritableChannel, Callback)}.
   */
  private class ReadIntoCallbackWrapper {
    /**
     * The {@link Future} where the result of {@link #readInto(AsyncWritableChannel, Callback)} will eventually be
     * updated.
     */
    public final FutureResult<Long> futureResult = new FutureResult<Long>();

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
