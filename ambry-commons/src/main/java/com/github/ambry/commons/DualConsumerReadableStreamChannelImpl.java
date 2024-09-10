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
 */

package com.github.ambry.commons;

import com.github.ambry.router.AsyncWritableChannel;
import com.github.ambry.router.FutureResult;
import com.github.ambry.router.ReadableStreamChannel;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A channel that acts as an intermediary for a stream of bytes to be read by two destinations asynchronously.
 */
public class DualConsumerReadableStreamChannelImpl implements ReadableStreamChannel {

  public static final Logger logger = LoggerFactory.getLogger(DualConsumerReadableStreamChannelImpl.class);
  private ReadableStreamChannel source;
  // Primary reader of this channel
  private final AsyncWritableChannel primaryDest;
  // Secondary reader of this channel
  private final AsyncWritableChannel secondaryDest;
  // The {@link Callback} that will be invoked either when all the data in the channel has been emptied into primary
  private Callback<Long> primaryDestFinalCallback = null;
  // The {@link Callback} that will be invoked either when all the data in the channel has been emptied into secondary
  private Callback<Long> secondaryDestFinalCallback = null;
  // The primary {@link Future} that will eventually contain the result of the operation.
  private final FutureResult<Long> primaryDestFutureResult = new FutureResult<>();
  // The secondary {@link Future} that will eventually contain the result of the operation.
  private final FutureResult<Long> secondaryDestFutureResult = new FutureResult<>();
  // Indicates if primary started reading
  private volatile boolean primaryDestReadCalled = false;
  // Indicates if secondary has started reading
  private volatile boolean secondaryDestReadCalled = false;

  // Chunks buffered for Primary
  private final Queue<ChunkInfo> requestContentsForPrimary = new LinkedBlockingQueue<>();
  // Chunks buffered for secondary
  private final Queue<ChunkInfo> requestContentsForSecondary = new LinkedBlockingQueue<>();

  // Chunks that have been read by primary
  private final Queue<ResolvedChunkInfo> primaryResolvedChunks = new LinkedBlockingQueue<>();
  // Chunks that have been read by secondary
  private final Queue<ResolvedChunkInfo> secondaryResolvedChunks = new LinkedBlockingQueue<>();

  // Indicates if this readable channel is still open
  private final AtomicBoolean channelOpen = new AtomicBoolean(true);

  // Lock for writing contents to primary and secondary
  private final ReentrantLock contentLock = new ReentrantLock();
  // Lock for co-ordinating callbacks on chunks that have been read
  private final Lock destCallbackLock = new ReentrantLock();
  // Condition
  private final Condition destCallbackArrived = destCallbackLock.newCondition();
  private volatile boolean destCallbackTimedOut = false;
  private volatile boolean secondaryDestClosed = false;
  // TODO: Make it configurable
  private final int destCallbackTimeOutMs = 500;

  /**
   *
   * @param source the channel to read the data from.
   * @param primaryDest the primary {@link AsyncWritableChannel} to read the data into.
   * @param secondaryDest the secondary {@link AsyncWritableChannel} to read the data into.
   */
  public DualConsumerReadableStreamChannelImpl(ReadableStreamChannel source, AsyncWritableChannel primaryDest,
      AsyncWritableChannel secondaryDest) {
    this.source = source;
    this.primaryDest = primaryDest;
    this.secondaryDest = secondaryDest;

    AsyncWritableChannel localAsyncWritableChannel = new AsyncWritableChannelImpl();
    source.readInto(localAsyncWritableChannel, (result, exception) -> {
      if (primaryDestFinalCallback != null) {
        primaryDestFutureResult.done(result, exception);
        primaryDestFinalCallback.onCompletion(result, exception);
      }

      if (secondaryDestFinalCallback != null) {
        secondaryDestFutureResult.done(result, exception);
        secondaryDestFinalCallback.onCompletion(result, exception);
      }
    });

    // Start a background thread for handle write callbacks from both the destinations
    Executor eventExecutor = Executors.newSingleThreadExecutor();
    eventExecutor.execute(new ChunkResolverThread());
  }

  @Override
  public long getSize() {
    return -1;
  }

  @Override
  public Future<Long> readInto(AsyncWritableChannel asyncWritableChannel, Callback<Long> callback) {
    contentLock.lock();
    try {
      if (asyncWritableChannel == primaryDest) {
        logger.debug("ReadInto called from primary destination. Size of buffered contents {}",
            requestContentsForPrimary.size());
        primaryDestReadCalled = true;
        primaryDestFinalCallback = callback;
        while (!requestContentsForPrimary.isEmpty()) {
          ChunkInfo chunkInfo = requestContentsForPrimary.poll();
          primaryDest.write(chunkInfo.buf,
              new ChunkWriteCallbackWrapper(chunkInfo.srcCallback, true, chunkInfo.future));
        }
        return primaryDestFutureResult;
      } else if (asyncWritableChannel == secondaryDest) {
        logger.debug("ReadInto called from secondary destination. Size of buffered contents {}",
            requestContentsForSecondary.size());
        secondaryDestReadCalled = true;
        secondaryDestFinalCallback = callback;

        while (!requestContentsForSecondary.isEmpty()) {
          ChunkInfo chunkInfo = requestContentsForSecondary.poll();
          secondaryDest.write(chunkInfo.buf,
              new ChunkWriteCallbackWrapper(chunkInfo.srcCallback, false, chunkInfo.future));
        }

        return secondaryDestFutureResult;
      }
    } finally {
      contentLock.unlock();
    }
    return null;
  }

  @Override
  public boolean isOpen() {
    return false;
  }

  @Override
  public void close() throws IOException {

  }

  /**
   * Writable channel that acts as proxy between source (input readable stream) and destinations (AsyncWritableChannels)
   */
  class AsyncWritableChannelImpl implements AsyncWritableChannel {

    @Override
    public Future<Long> write(ByteBuffer src, Callback<Long> callback) {
      return write(Unpooled.wrappedBuffer(src), callback);
    }

    @Override
    public Future<Long> write(ByteBuf src, Callback<Long> callback) {
      contentLock.lock();
      try {
        FutureResult<Long> futureResult = new FutureResult<>();
        ByteBuf duplicate = src.duplicate();
        if (primaryDestReadCalled) {
          ChunkWriteCallbackWrapper chunkWriteCallbackWrapper =
              new ChunkWriteCallbackWrapper(callback, true, futureResult);
          primaryDest.write(src, chunkWriteCallbackWrapper);
        } else {
          requestContentsForPrimary.add(new ChunkInfo(src, callback, futureResult));
        }

        if (secondaryDestReadCalled) {
          secondaryDest.write(duplicate, new ChunkWriteCallbackWrapper(callback, false, futureResult));
        } else {
          requestContentsForSecondary.add(new ChunkInfo(duplicate, callback, futureResult));
        }

        return futureResult;
      } finally {
        contentLock.unlock();
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

  class ChunkWriteCallbackWrapper implements Callback<Long> {

    private final Callback<Long> srcCallback;
    boolean isPrimary;
    // The future that will be set after chunk is written successfully.
    public final FutureResult<Long> future;

    public ChunkWriteCallbackWrapper(Callback<Long> srcCallback, boolean isPrimary, FutureResult<Long> futureResult) {
      this.srcCallback = srcCallback;
      this.isPrimary = isPrimary;
      future = futureResult;
    }

    @Override
    public void onCompletion(Long result, Exception exception) {
      destCallbackLock.lock();
      try {
        ResolvedChunkInfo resolvedChunkInfo = new ResolvedChunkInfo(srcCallback, result, exception, future);
        if (isPrimary) {
          primaryResolvedChunks.add(resolvedChunkInfo);
        } else {
          secondaryResolvedChunks.add(resolvedChunkInfo);
        }
        // Signal the resolver thread that callback from one of the destinations has arrived
        destCallbackArrived.signal();
      } finally {
        destCallbackLock.unlock();
      }
    }
  }

  private static class ChunkInfo {

    private final ByteBuf buf;
    private final Callback<Long> srcCallback;
    // The future that will be set after chunk is written successfully.
    public final FutureResult<Long> future;

    /**
     * Create a new instance of ChunkData with the given parameters.
     *
     * @param buf         the bytes of data associated with the chunk.
     * @param srcCallback the {@link Callback} that will be invoked on chunk resolution.
     * @param future      the future that will be set on chunk resolution.
     */
    private ChunkInfo(ByteBuf buf, Callback<Long> srcCallback, FutureResult<Long> future) {
      this.buf = buf;
      this.srcCallback = srcCallback;
      this.future = future;
    }
  }

  private static class ResolvedChunkInfo {

    private final Callback<Long> srcCallback;
    private final long bytesRead;
    private final Exception exception;
    // The future that will be set after chunk is written successfully.
    public final FutureResult<Long> future;

    public ResolvedChunkInfo(Callback<Long> srcCallback, long bytesRead, Exception exception,
        FutureResult<Long> future) {
      this.srcCallback = srcCallback;
      this.bytesRead = bytesRead;
      this.exception = exception;
      this.future = future;
    }
  }

  /**
   * A background thread to handle chunk write callbacks from both primary and secondary destinations. Once both
   * destinations acknowledge the writes, input readable stream channel callback is invoked.
   * TODO: Throw error if bytes read in both destination callbacks are not the same.
   */
  class ChunkResolverThread implements Runnable {

    @Override
    public void run() {
      while (channelOpen.get()) {
        destCallbackLock.lock();
        try {
          ResolvedChunkInfo primaryResolvedChunk = primaryResolvedChunks.peek();
          ResolvedChunkInfo secondaryResolvedChunk = secondaryResolvedChunks.peek();

          if (primaryResolvedChunk == null) {
            destCallbackArrived.await();
          } else if (!secondaryDestClosed && secondaryResolvedChunk == null) {
            if (!destCallbackArrived.await(destCallbackTimeOutMs, TimeUnit.MILLISECONDS)) {
              destCallbackTimedOut = true;
            }
          } else {
            // Both the destinations acknowledge chunk write. Invoke origin callback.
            primaryResolvedChunks.poll();
            secondaryResolvedChunks.poll();
            Callback<Long> srcCallback = primaryResolvedChunk.srcCallback;
            long bytesRead = primaryResolvedChunk.bytesRead;
            Exception exception = primaryResolvedChunk.exception;
            primaryResolvedChunk.future.done(bytesRead, exception);
            srcCallback.onCompletion(bytesRead, exception);
          }
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        } finally {

          if (destCallbackTimedOut && !secondaryDestClosed) {
            try {
              // If secondary destination write completion callback didn't arrive in time, close the secondary.
              logger.error("Secondary destination timed out. Closing secondary");
              secondaryDestClosed = true;
              secondaryDest.close();
            } catch (IOException e) {
              logger.error("Exception while closing secondary", e);
            }
          }

          destCallbackLock.unlock();
        }
      }
    }
  }
}
