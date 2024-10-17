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
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Queue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A channel that is used to pipe the bytes written to it to underlying {@link ReadableStreamChannel}s. The bytes sent
 * to different {@link ReadableStreamChannel} can be read into corresponding destinations. For now, this implementation
 * only supports 2 {@link ReadableStreamChannel}(s), primary and secondary. If there is an error in reading to secondary,
 * the secondary channel is closed and the piping is short-circuited to only primary
 */
public class PipedAsyncWritableChannel implements AsyncWritableChannel {

  private final ReadableStreamChannel sourceChannel;
  private final int secondaryTimeoutInMs;
  private final PipedReadableStreamChannel pipedPrimaryReadChannel;
  private PipedReadableStreamChannel pipedSecondaryReadChannel = null;
  private final ReentrantLock lock = new ReentrantLock();
  private final AtomicBoolean channelOpen = new AtomicBoolean(true);
  private static final Logger logger = LoggerFactory.getLogger(PipedAsyncWritableChannel.class);
  final static HashedWheelTimer wheel = new HashedWheelTimer(10, TimeUnit.MILLISECONDS, 1024);

  /**
   * @param sourceChannel        The channel that contains the stream of bytes to be read into primary and secondary
   *                             destinations
   * @param withSecondary        if {@code true}, sends the bytes from source channel to secondary destination as well
   * @param secondaryTimeoutInMs time in milliseconds after which secondary read channel will be closed
   */
  public PipedAsyncWritableChannel(ReadableStreamChannel sourceChannel, boolean withSecondary,
      int secondaryTimeoutInMs) {
    this.sourceChannel = sourceChannel;
    this.secondaryTimeoutInMs = secondaryTimeoutInMs;

    pipedPrimaryReadChannel = new PipedReadableStreamChannel(true);
    if (withSecondary) {
      pipedSecondaryReadChannel = new PipedReadableStreamChannel(false);
      wheel.start();
    }

    sourceChannel.readInto(this, (result, exception) -> {
      // Forward the callback to writable channels of piped readable channels
      pipedPrimaryReadChannel.invokeFinalCallback(exception);
      if (pipedSecondaryReadChannel != null) {
        pipedSecondaryReadChannel.invokeFinalCallback(exception);
      }
      // Close this writable channel. It will close the piped readable channels as well.
      close();
    });
  }

  /**
   * @return the primary {@link ReadableStreamChannel} that contains the bytes coming into this writable channel
   */
  public ReadableStreamChannel getPrimaryReadableStreamChannel() {
    return pipedPrimaryReadChannel;
  }

  /**
   * @return the secondary {@link ReadableStreamChannel} that contains the bytes coming into this writable channel. If
   * this class was created without secondary destination, this will return null
   */
  public ReadableStreamChannel getSecondaryReadableStreamChannel() {
    return pipedSecondaryReadChannel;
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
      ChunkData chunkData = new ChunkData(src, callback);
      if (!isOpen()) {
        // This writable channel is no longer open. Return with closed channel exception
        logger.error("Cannot be written to this channel. Writable channel is already closed");
        chunkData.resolveChunk(0, new ClosedChannelException());
      } else {
        // Forward the bytes to underlying primary and secondary readable channels. The readable channels will read the
        // bytes into their respective destination writable channels
        pipedPrimaryReadChannel.writeToChannel(chunkData);
        if (pipedSecondaryReadChannel != null) {
          pipedSecondaryReadChannel.writeToChannel(chunkData);
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
  public void close() {
    if (channelOpen.compareAndSet(true, false)) {
      pipedPrimaryReadChannel.close();
      if (pipedSecondaryReadChannel != null) {
        pipedSecondaryReadChannel.close();
      }
    }
  }

  /**
   * Close secondary alone with an exception
   * @param exception exception
   */
  public void closeSecondary(Exception exception) {
    if (pipedSecondaryReadChannel != null) {
      pipedSecondaryReadChannel.invokeFinalCallback(exception);
      pipedSecondaryReadChannel.close();
      pipedSecondaryReadChannel = null;
    }
  }

  /**
   * A channel that contains the bytes written into parent class {@link PipedAsyncWritableChannel}
   */
  private class PipedReadableStreamChannel implements ReadableStreamChannel {
    private AsyncWritableChannel writableChannel;
    private Callback<Long> writableChannelFinalCallback;
    private final boolean primary;
    public final FutureResult<Long> writableChannelFutureResult = new FutureResult<>();
    private final AtomicBoolean channelOpen = new AtomicBoolean(true);
    private final Queue<ChunkData> bufferedChunks = new LinkedBlockingQueue<>();
    private final AtomicLong totalBytesRead = new AtomicLong(0);
    private final AtomicBoolean callbackInvoked = new AtomicBoolean(false);

    public PipedReadableStreamChannel(boolean primary) {
      this.primary = primary;
    }

    @Override
    public long getSize() {
      return sourceChannel.getSize();
    }

    @Override
    public Future<Long> readInto(AsyncWritableChannel asyncWritableChannel, Callback<Long> callback) {
      lock.lock();
      try {
        if (this.writableChannel != null) {
          throw new IllegalStateException("AsyncWritableChannel already exist for this ReadableStreamChannel");
        }
        if (!isOpen()) {
          logger.error("Cannot be read from this channel. Readable channel is already closed");
          if (callback != null) {
            callback.onCompletion(0L, new ClosedChannelException());
          }
          writableChannelFutureResult.done(0L, new ClosedChannelException());
        } else {
          this.writableChannel = asyncWritableChannel;
          this.writableChannelFinalCallback = callback;
          ChunkData chunkData;
          while ((chunkData = bufferedChunks.poll()) != null) {
            writeToChannel(chunkData);
          }
        }
        return writableChannelFutureResult;
      } finally {
        lock.unlock();
      }
    }

    /**
     * Writes the content to the provided {@link AsyncWritableChannel} channel
     * @param chunkData Representation of all the data associated with a chunk.
     */
    private void writeToChannel(ChunkData chunkData) {
      lock.lock();
      try {
        if (writableChannel == null) {
          // No write channel registered yet. Buffer the chunk
          bufferedChunks.add(chunkData);
          return;
        }
        writableChannel.write(chunkData.buf.duplicate(),
            chunkData.makeCallbackForReadableStreamChannel(primary, totalBytesRead));
      } finally {
        lock.unlock();
      }
    }

    /**
     * Invokes the final callback and updates the future of registered {@link AsyncWritableChannel}.
     * @param exception the {@link Exception}, if any, to pass to the callback.
     */
    private void invokeFinalCallback(Exception exception) {
      if (callbackInvoked.compareAndSet(false, true)) {
        if (writableChannel != null) {
          writableChannelFutureResult.done(totalBytesRead.get(), exception);
          if (writableChannelFinalCallback != null) {
            writableChannelFinalCallback.onCompletion(totalBytesRead.get(), exception);
          }
        }
      }
    }

    @Override
    public boolean isOpen() {
      return channelOpen.get();
    }

    @Override
    public void close() {
      lock.lock();
      try {
        if (channelOpen.compareAndSet(true, false)) {
          ChunkData chunkData;
          while ((chunkData = bufferedChunks.poll()) != null) {
            if (primary) {
              // Only resolve chunk for primary channel. This will invoke source ReadableStreamChannel callback
              chunkData.resolveChunk(0, new ClosedChannelException());
            }
          }
        }
      } finally {
        lock.unlock();
      }
    }
  }

  /**
   * Representation of all the data associated with a chunk i.e. the actual bytes and the future and callback that need to
   * be invoked on resolution.
   */
  private class ChunkData {
    // The future that will be set on chunk resolution
    public final FutureResult<Long> future = new FutureResult<>();
    public ByteBuf buf;
    private final Callback<Long> callback;
    private Result primaryWriteCallbackResult;
    private Result secondaryWriteCallbackResult;
    private final Lock lock = new ReentrantLock();
    private final AtomicBoolean chunkResolved = new AtomicBoolean(false);
    private Timeout secondaryTimeout = null;

    /**
     * Create a new instance of ChunkData with the given parameters.
     * @param buf            the bytes of data associated with the chunk.
     * @param callback       the {@link Callback} that will be invoked on chunk resolution.
     */
    private ChunkData(ByteBuf buf, Callback<Long> callback) {
      this.buf = buf;
      this.callback = callback;
    }

    /**
     * Marks a chunk as handled and invokes the callback and future that accompanied this chunk of data. Once a chunk is
     * resolved, the data inside it is considered void.
     * @param bytesRead num of bytes read into write channels
     * @param exception the reason for chunk handling failure.
     */
    private void resolveChunk(long bytesRead, Exception exception) {
      if (chunkResolved.compareAndSet(false, true)) {
        future.done(bytesRead, exception);
        if (callback != null) {
          callback.onCompletion(bytesRead, exception);
        }
      } else {
        logger.error("Chunk is already resolved. Attempting duplicate resolution");
      }
    }

    private Callback<Long> makeCallbackForReadableStreamChannel(boolean primary, AtomicLong totalBytesRead) {
      return (result, exception) -> {
        lock.lock();
        try {
          if (chunkResolved.get()) {
            logger.debug("Chunk is already resolved. Must be callback from secondary after time out. Do nothing");
            return;
          }

          if (exception == null) {
            totalBytesRead.getAndAdd(result);
          }

          if (primary) {
            // This callback is coming from primary reader
            primaryWriteCallbackResult = new Result(result, exception);
          } else {
            // This callback is coming from secondary reader
            secondaryWriteCallbackResult = new Result(result, exception);
            if (secondaryTimeout != null) {
              secondaryTimeout.cancel();
            }
          }

          if (PipedAsyncWritableChannel.this.pipedSecondaryReadChannel == null) {
            // Either there is no secondary or secondary failed with an exception. Invoke "sourceChannel" callback
            // informing that reading of these bytes is completed. This will send more bytes
            resolveChunk(primaryWriteCallbackResult.bytesWritten, primaryWriteCallbackResult.exception);
          } else {
            if (primaryWriteCallbackResult != null && secondaryWriteCallbackResult != null) {
              // Both primary and secondary callback came. Invoke "sourceChannel" callback informing that
              // reading of these bytes is completed. This will send more bytes
              if (primaryWriteCallbackResult.exception != null || secondaryWriteCallbackResult.exception != null) {
                // If primary or secondary fails, close the secondary with closed channel exception.
                String message = primaryWriteCallbackResult.exception != null ? "Primary failed with an exception "
                    + primaryWriteCallbackResult.exception
                    : "Secondary failed with an exception " + secondaryWriteCallbackResult.exception
                        + ". Closing secondary channel";
                logger.error(message);
                PipedAsyncWritableChannel.this.closeSecondary(new ClosedChannelException());
              }
              resolveChunk(primaryWriteCallbackResult.bytesWritten, primaryWriteCallbackResult.exception);
            } else if (primaryWriteCallbackResult != null) {
              secondaryTimeout = wheel.newTimeout(timeout -> {
                // Secondary took very long
                logger.error("Closing secondary channel since it is unresponsive");
                PipedAsyncWritableChannel.this.closeSecondary(new ClosedChannelException());
                resolveChunk(primaryWriteCallbackResult.bytesWritten, primaryWriteCallbackResult.exception);
              }, secondaryTimeoutInMs, TimeUnit.MILLISECONDS);
            }
          }
        } finally {
          lock.unlock();
        }
      };
    }
  }

  private static class Result {
    private final long bytesWritten;
    private final Exception exception;

    private Result(long bytesWritten, Exception exception) {
      this.bytesWritten = bytesWritten;
      this.exception = exception;
    }
  }
}
