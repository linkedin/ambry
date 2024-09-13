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
 *
 */
package com.github.ambry.commons;

import com.github.ambry.router.AsyncWritableChannel;
import com.github.ambry.router.FutureResult;
import com.github.ambry.router.ReadableStreamChannel;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class PipedAsyncWritableChannel implements AsyncWritableChannel {

  private final List<ChunkData> chunks = new ArrayList<ChunkData>();
  private final ReentrantLock lock = new ReentrantLock();
  private final AtomicBoolean channelOpen = new AtomicBoolean(true);

  private final List<PipedReadableStreamChannel> pipedReadableStreamChannels = new ArrayList<>();

  public PipedAsyncWritableChannel(int numReadabledStreamChannel) {
    if (numReadabledStreamChannel <= 0) {
      throw new IllegalArgumentException(
          "Number of readable stream channel is not valid: " + numReadabledStreamChannel);
    }
    for (int i = 0; i < numReadabledStreamChannel; i++) {
      pipedReadableStreamChannels.add(new PipedReadableStreamChannel());
    }
  }

  public ReadableStreamChannel getReadableStreamChannel(int idx) {
    if (idx >= pipedReadableStreamChannels.size()) {
      throw new IllegalArgumentException("Index " + idx + " is greater than the number of readable stream channels");
    }
    return pipedReadableStreamChannels.get(idx);
  }

  @Override
  public Future<Long> write(ByteBuffer src, Callback<Long> callback) {
    if (src == null) {
      throw new IllegalArgumentException("Source buffer cannot be null");
    }
    return write(Unpooled.wrappedBuffer(src), callback);
  }

  @Override
  public Future<Long> write(ByteBuf src, Callback<Long> callback) {
    if (src == null) {
      throw new IllegalArgumentException("Source buffer cannot be null");
    }
    lock.lock();
    try {
      ChunkData chunkData = new ChunkData(src, callback, pipedReadableStreamChannels.size());
      chunks.add(chunkData);
      if (!isOpen()) {
        resolveAllRemainingChunks(new ClosedChannelException());
      } else {
        for (PipedReadableStreamChannel channel : pipedReadableStreamChannels) {
          if (channel.hasWritableChannel()) {
            channel.writeToChannel();
          }
        }
      }
      return chunkData.future;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean isOpen() {
    return channelOpen.get();
  }

  @Override
  public void close() throws IOException {
    if (channelOpen.compareAndSet(true, false)) {
      resolveAllRemainingChunks(new ClosedChannelException());
    }
  }

  /**
   * Resolves all the remaining chunks with {@link ClosedChannelException}.
   * @param e the exception to use to resolve all the chunks.
   */
  private void resolveAllRemainingChunks(Exception e) {
    lock.lock();
    try {
      for (ChunkData chunk : chunks) {
        chunk.resolveChunk(e);
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Representation of all the data associated with a chunk i.e. the actual bytes and the future and callback that need to
   * be invoked on resolution.
   */
  private static class ChunkData {
    /**
     * The future that will be set on chunk resolution.
     */
    public final FutureResult<Long> future = new FutureResult<Long>();
    public ByteBuf buf;
    private Exception finalException = null;
    private final Callback<Long> callback;
    private final long size;
    private final List<Exception> callbackExceptions = new ArrayList<>();
    private final int expectCbCount;
    private final Lock lock = new ReentrantLock();

    /**
     * Create a new instance of ChunkData with the given parameters.
     * @param buf the bytes of data associated with the chunk.
     * @param callback the {@link Callback} that will be invoked on chunk resolution.
     */
    private ChunkData(ByteBuf buf, Callback<Long> callback, int expectCbCount) {
      this.buf = buf;
      if (buf != null) {
        size = buf.readableBytes();
      } else {
        size = 0;
      }
      this.callback = callback;
      this.expectCbCount = expectCbCount;
    }

    /**
     * Marks a chunk as handled and invokes the callback and future that accompanied this chunk of data. Once a chunk is
     * resolved, the data inside it is considered void.
     * @param exception the reason for chunk handling failure.
     */
    private void resolveChunk(Exception exception) {
      if (buf != null) {
        future.done(exception == null ? size : 0, exception);
        if (callback != null) {
          callback.onCompletion(exception == null ? size : 0, exception);
        }
        buf = null;
      }
    }

    private Callback makeCallbackForReadableStreamChannel() {
      return (result, exception) -> {
        lock.lock();
        try {
          callbackExceptions.add(exception);
          if (exception != null) {
            finalException = exception;
          }
          if (callbackExceptions.size() == expectCbCount) {
            future.done(finalException == null ? size : 0, finalException);
            if (callback != null) {
              callback.onCompletion(finalException == null ? size : 0, finalException);
            }
            buf = null;
          }
        } finally {
          lock.unlock();
        }
      };
    }
  }

  private class PipedReadableStreamChannel implements ReadableStreamChannel {
    private AsyncWritableChannel writableChannel;
    private Callback<Long> callback;
    private int chunkIdx = 0;

    public boolean hasWritableChannel() {
      lock.lock();
      try {
        return writableChannel != null;
      } finally {
        lock.unlock();
      }
    }

    @Override
    public long getSize() {
      return 0;
    }

    @Override
    public Future<Long> readInto(AsyncWritableChannel asyncWritableChannel, Callback<Long> callback) {
      if (this.writableChannel != null) {
        throw new IllegalStateException("AsyncWritableChannel already exist for this ReadableStreamChannel");
      }
      lock.lock();
      this.writableChannel = asyncWritableChannel;
      this.callback = callback;
      lock.unlock();
      return writeToChannel();
    }

    private Future<Long> writeToChannel() {
      lock.lock();
      try {
        FutureResult<Long> future = new FutureResult<>();
        if (!isOpen()) {
          Exception closedException = new ClosedChannelException();
          this.callback.onCompletion(0L, closedException);
          future.done(0L, closedException);
          return future;
        }
        for (; chunkIdx < chunks.size(); chunkIdx++) {
          ChunkData chunkData = chunks.get(chunkIdx);
          writableChannel.write(chunkData.buf.retainedDuplicate(), chunkData.makeCallbackForReadableStreamChannel());
        }
        return future;
      } finally {
        lock.unlock();
      }
    }

    @Override
    public boolean isOpen() {
      return false;
    }

    @Override
    public void close() throws IOException {

    }
  }
}
