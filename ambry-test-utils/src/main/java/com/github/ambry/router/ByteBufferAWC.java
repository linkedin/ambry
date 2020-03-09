/*
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

import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Queue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;


/**
 * An implementation of {@link AsyncWritableChannel} that queues the buffers received and waits for them to be
 * retrieved and resolved by an external thread.
 */
public class ByteBufferAWC implements AsyncWritableChannel {
  private final LinkedBlockingQueue<ChunkData> chunks = new LinkedBlockingQueue<ChunkData>();
  private final Queue<ChunkData> chunksAwaitingResolution = new LinkedBlockingQueue<ChunkData>();
  private final ReentrantLock lock = new ReentrantLock();
  private final AtomicBoolean channelOpen = new AtomicBoolean(true);

  /**
   * If the channel is open, simply queues the buffer to be handled later.
   * @param src the data that needs to be written to the channel.
   * @param callback the {@link Callback} that will be invoked once the write succeeds/fails. This can be null.
   * @return a {@link Future} that will eventually contain the result of the write operation.
   * @throws IllegalArgumentException if {@code src} is null.
   */
  @Override
  public Future<Long> write(ByteBuffer src, Callback<Long> callback) {
    if (src == null) {
      throw new IllegalArgumentException("Source buffer cannot be null");
    }
    ChunkData chunkData = new ChunkData(src, callback);
    chunks.add(chunkData);
    if (!isOpen()) {
      resolveAllRemainingChunks(new ClosedChannelException());
    }
    return chunkData.future;
  }

  @Override
  public boolean isOpen() {
    return channelOpen.get();
  }

  /**
   * Closes the channel and resolves all pending chunks with a {@link ClosedChannelException}. Also queues a poison
   * so that {@link #getNextChunk()} starts returning {@code null}.
   */
  @Override
  public void close() {
    if (channelOpen.compareAndSet(true, false)) {
      resolveAllRemainingChunks(new ClosedChannelException());
    }
  }

  /**
   * Gets the next chunk of data as a {@link ByteBuffer} when it is available.
   * <p/>
   * If the channel is not closed, this function blocks until the next chunk is available. Once the channel is closed,
   * this function starts returning {@code null}.
   * @return a {@link ByteBuffer} representing the next chunk of data if the channel is not closed. {@code null} if the
   *         channel is closed.
   * @throws InterruptedException if the wait for a chunk is interrupted.
   */
  public ByteBuffer getNextChunk() throws InterruptedException {
    ByteBuffer chunkBuf = null;
    if (isOpen()) {
      chunkBuf = getChunkBuffer(chunks.take());
    }
    return chunkBuf;
  }

  /**
   * Gets the next chunk of data as a {@link ByteBuffer}.
   * <p/>
   * If the channel is not closed, this function waits for {@code timeoutInMs} ms for a chunk. If the channel is closed
   * or if {@code timeoutInMs} expires, this function returns {@code null}.
   * @param timeoutInMs the time in ms to wait for a chunk.
   * @return a {@link ByteBuffer} representing the next chunk of data if the channel is not closed and a chunk becomes
   *          available within {@code timeoutInMs}. {@code null} if the channel is closed or if {@code timeoutInMs}
   *          expires.
   * @throws InterruptedException if the wait for a chunk is interrupted.
   */
  public ByteBuffer getNextChunk(long timeoutInMs) throws InterruptedException {
    ByteBuffer chunkBuf = null;
    if (isOpen()) {
      chunkBuf = getChunkBuffer(chunks.poll(timeoutInMs, TimeUnit.MILLISECONDS));
    }
    return chunkBuf;
  }

  /**
   * Resolves the oldest "checked-out" chunk and invokes the callback and future that accompanied the chunk. Once a
   * chunk is resolved, the data inside it is considered void. If no chunks have been "checked-out" yet, does nothing.
   * @param exception any {@link Exception} that occurred during the handling that needs to be notified.
   */
  public void resolveOldestChunk(Exception exception) {
    ChunkData chunkData = chunksAwaitingResolution.poll();
    if (chunkData != null) {
      chunkData.resolveChunk(exception);
    }
  }

  /**
   * Gets the buffer associated with the chunk if there is one. Also updates internal state.
   * @param chunkData the data associated with the chunk whose buffer needs to be returned.
   * @return the buffer inside {@code chunkData} if there is one.
   */
  private ByteBuffer getChunkBuffer(ChunkData chunkData) {
    ByteBuffer chunkBuf = null;
    if (chunkData != null && chunkData.buffer != null) {
      chunkBuf = chunkData.buffer;
      chunksAwaitingResolution.add(chunkData);
      if (!isOpen()) {
        chunkBuf = null;
        resolveAllRemainingChunks(new ClosedChannelException());
      }
    }
    return chunkBuf;
  }

  /**
   * Resolves all the remaining chunks with {@link ClosedChannelException}.
   * @param e the exception to use to resolve all the chunks.
   */
  private void resolveAllRemainingChunks(Exception e) {
    lock.lock();
    try {
      ChunkData chunkData = chunksAwaitingResolution.poll();
      while (chunkData != null) {
        chunkData.resolveChunk(e);
        chunkData = chunksAwaitingResolution.poll();
      }
      chunkData = chunks.poll();
      while (chunkData != null) {
        chunkData.resolveChunk(e);
        chunkData = chunks.poll();
      }
      chunks.add(new ChunkData(null, null));
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
    /**
     * The bytes associated with this chunk.
     */
    public final ByteBuffer buffer;

    private final int startPos;
    private final Callback<Long> callback;

    /**
     * Create a new instance of ChunkData with the given parameters.
     * @param buffer the bytes of data associated with the chunk.
     * @param callback the {@link Callback} that will be invoked on chunk resolution.
     */
    public ChunkData(ByteBuffer buffer, Callback<Long> callback) {
      this.buffer = buffer;
      if (buffer != null) {
        startPos = buffer.position();
      } else {
        startPos = 0;
      }
      this.callback = callback;
    }

    /**
     * Marks a chunk as handled and invokes the callback and future that accompanied this chunk of data. Once a chunk is
     * resolved, the data inside it is considered void.
     * @param exception the reason for chunk handling failure.
     */
    public void resolveChunk(Exception exception) {
      if (buffer != null) {
        long bytesWritten = buffer.position() - startPos;
        future.done(bytesWritten, exception);
        if (callback != null) {
          callback.onCompletion(bytesWritten, exception);
        }
      }
    }
  }
}
