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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
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
public class ByteBufferAsyncWritableChannel implements AsyncWritableChannel {
  private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocate(0);

  /**
   * List of events of interest to the consumer of the content in this channel.
   */
  public enum EventType {
    Write, Close,
  }

  /**
   * Callback that can be used to listen to events that happen inside this channel.
   */
  public interface ChannelEventListener {
    /**
     * Called when an event of the given type completes within this channel.
     * @param e the {@link EventType} of the event that completed.
     */
    void onEvent(EventType e);
  }

  private final LinkedBlockingQueue<ChunkData> chunks = new LinkedBlockingQueue<ChunkData>();
  private final Queue<ChunkData> chunksAwaitingResolution = new LinkedBlockingQueue<ChunkData>();
  private final ReentrantLock lock = new ReentrantLock();
  private final AtomicBoolean channelOpen = new AtomicBoolean(true);
  private final ChannelEventListener channelEventListener;

  /**
   * Construct a ByteBufferAsyncWritableChannel with a null {@link ChannelEventListener}
   */
  public ByteBufferAsyncWritableChannel() {
    this(null);
  }

  /**
   * Construct a ByteBufferAsyncWritableChannel with the given {@link ChannelEventListener}
   * @param channelEventListener the {@link ChannelEventListener} that will be notified on consumable events.
   */
  public ByteBufferAsyncWritableChannel(ChannelEventListener channelEventListener) {
    this.channelEventListener = channelEventListener;
  }

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
    return write(Unpooled.wrappedBuffer(src), callback);
  }

  /**
   * If the channel is open, simply queues the buffer to be handled later.
   * @param src the data that needs to be written to the channel.
   * @param callback the {@link Callback} that will be invoked once the write succeeds/fails. This can be null.
   * @return a {@link Future} that will eventually contain the result of the write operation.
   * @throws IllegalArgumentException if {@code src} is null.
   */
  @Override
  public Future<Long> write(ByteBuf src, Callback<Long> callback) {
    if (src == null) {
      throw new IllegalArgumentException("Source buffer cannot be null");
    }
    ChunkData chunkData = new ChunkData(src, callback);
    chunks.add(chunkData);
    if (channelEventListener != null) {
      channelEventListener.onEvent(EventType.Write);
    }
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
    if (channelEventListener != null) {
      channelEventListener.onEvent(EventType.Close);
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
    ByteBuf chunkBuf = getNextByteBuf();
    if (chunkBuf == null) {
      return null;
    }
    return convertToByteBuffer(chunkBuf);
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
    ByteBuf chunkBuf = getNextByteBuf(timeoutInMs);
    if (chunkBuf == null) {
      return null;
    }
    return convertToByteBuffer(chunkBuf);
  }

  /**
   * Convert the {@link ByteBuf} to a {@link ByteBuffer}. It will change the {@link ByteBuf#readerIndex()} for the given
   * {@code buf}.
   * @param buf The {@link ByteBuf}'s data that would be converted to a {@link ByteBuffer}.
   * @return A {@link ByteBuffer} that contains the same data as the given {@code buf}.
   */
  private ByteBuffer convertToByteBuffer(ByteBuf buf) {
    if (buf == null) {
      return null;
    }
    int bufferCount = buf.nioBufferCount();
    switch (bufferCount) {
      case 0:
        return EMPTY_BYTE_BUFFER;
      case 1:
        ByteBuffer buffer = buf.nioBuffer();
        buf.readerIndex(buf.readerIndex() + buf.readableBytes());
        return buffer;
      default:
        ByteBuffer byteBuffer = ByteBuffer.allocate(buf.readableBytes());
        // This would also update the readerIndex for buf.
        buf.readBytes(byteBuffer);
        byteBuffer.flip();
        return byteBuffer;
    }
  }

  /**
   * Gets the next chunk of data as a {@link ByteBuf} when it is available.
   * <p/>
   * If the channel is not closed, this function blocks until the next chunk is available. Once the channel is closed,
   * this function starts returning {@code null}.
   * @return a {@link ByteBuf} representing the next chunk of data if the channel is not closed. {@code null} if the
   *         channel is closed.
   * @throws InterruptedException if the wait for a chunk is interrupted.
   */
  public ByteBuf getNextByteBuf() throws InterruptedException {
    ByteBuf chunkBuf = null;
    if (isOpen()) {
      chunkBuf = getChunkBuf(chunks.take());
    }
    return chunkBuf;
  }

  /**
   * Gets the next chunk of data as a {@link ByteBuf}.
   * <p/>
   * If the channel is not closed, this function waits for {@code timeoutInMs} ms for a chunk. If the channel is closed
   * or if {@code timeoutInMs} expires, this function returns {@code null}.
   * @param timeoutInMs the time in ms to wait for a chunk.
   * @return a {@link ByteBuf} representing the next chunk of data if the channel is not closed and a chunk becomes
   *          available within {@code timeoutInMs}. {@code null} if the channel is closed or if {@code timeoutInMs}
   *          expires.
   * @throws InterruptedException if the wait for a chunk is interrupted.
   */
  public ByteBuf getNextByteBuf(long timeoutInMs) throws InterruptedException {
    ByteBuf chunkBuf = null;
    if (isOpen()) {
      chunkBuf = getChunkBuf(chunks.poll(timeoutInMs, TimeUnit.MILLISECONDS));
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
   * Gets the {@link ByteBuf} associated with the chunk if there is one. Also updates internal state.
   * @param chunkData the data associated with the chunk whose buffer needs to be returned.
   * @return the buffer inside {@code chunkData} if there is one.
   */
  private ByteBuf getChunkBuf(ChunkData chunkData) {
    ByteBuf chunkBuf = null;
    if (chunkData != null && chunkData.buf != null) {
      chunkBuf = chunkData.buf;
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
    public ByteBuf buf;

    private final int startPos;
    private final Callback<Long> callback;

    /**
     * Create a new instance of ChunkData with the given parameters.
     * @param buf the bytes of data associated with the chunk.
     * @param callback the {@link Callback} that will be invoked on chunk resolution.
     */
    private ChunkData(ByteBuf buf, Callback<Long> callback) {
      this.buf = buf;
      if (buf != null) {
        startPos = buf.readerIndex();
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
    private void resolveChunk(Exception exception) {
      if (buf != null) {
        long bytesWritten = buf.readerIndex() - startPos;
        future.done(bytesWritten, exception);
        if (callback != null) {
          callback.onCompletion(bytesWritten, exception);
        }
      }
    }
  }
}