package com.github.ambry.commons;

import com.github.ambry.router.AsyncWritableChannel;
import com.github.ambry.router.Callback;
import com.github.ambry.router.FutureResult;
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
    lock.lock();
    try {
      if (!isOpen()) {
        chunkData.resolveChunk(new ClosedChannelException());
      } else {
        chunks.add(chunkData);
      }
    } finally {
      lock.unlock();
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
      lock.lock();
      try {
        resolveAllRemainingChunks(new ClosedChannelException());
        chunks.add(new ChunkData(null, null));
      } finally {
        lock.unlock();
      }
    }
  }

  /**
   * Gets the next chunk of data as a {@link ByteBuffer} when it is available.
   * <p/>
   * If the channel is not closed, this function blocks until the next chunk is available. Once the channel is closed,
   * this function starts returning {@code null}.
   * <p/>
   * This function is *not* meant to used concurrently and may cause unpredictable behaviour.
   * @return a {@link ByteBuffer} representing the next chunk of data if the channel is not closed. {@code null} if the
   *         channel is closed.
   * @throws InterruptedException if the wait for a chunk is interrupted.
   */
  public ByteBuffer getNextChunk()
      throws InterruptedException {
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
   * <p/>
   * This function is *not* meant to used concurrently and may cause unpredictable behaviour.
   * @param timeoutInMs the time in ms to wait for a chunk.
   * @return a {@link ByteBuffer} representing the next chunk of data if the channel is not closed and a chunk becomes
   *          available within {@code timeoutInMs}. {@code null} if the channel is closed or if {@code timeoutInMs}
   *          expires.
   * @throws InterruptedException if the wait for a chunk is interrupted.
   */
  public ByteBuffer getNextChunk(long timeoutInMs)
      throws InterruptedException {
    ByteBuffer chunkBuf = null;
    if (isOpen()) {
      chunkBuf = getChunkBuffer(chunks.poll(timeoutInMs, TimeUnit.MILLISECONDS));
    }
    return chunkBuf;
  }

  /**
   * Marks a chunk as handled and invokes the callback and future that accompanied this chunk of data. Once a chunk is
   * resolved, the data inside it is considered void.
   * <p/>
   * This function assumes that chunks are resolved in the same order that they were obtained.
   * <p/>
   * This function is *not* meant to used concurrently and may cause unpredictable behaviour.
   * @param chunkBuf the {@link ByteBuffer} that represents the chunk that was handled.
   * @param exception any {@link Exception} that occurred during the handling that needs to be notified.
   * @throws IllegalArgumentException if {@code chunk} is not a valid chunk that is eligible for resolution.
   */
  public void resolveChunk(ByteBuffer chunkBuf, Exception exception) {
    lock.lock();
    try {
      if (isOpen()) {
        if (chunksAwaitingResolution.peek() == null || chunksAwaitingResolution.peek().buffer != chunkBuf) {
          throw new IllegalArgumentException("Unrecognized chunk");
        }
        chunksAwaitingResolution.poll().resolveChunk(exception);
      }
    } finally {
      lock.unlock();
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
      lock.lock();
      try {
        if (isOpen()) {
          chunkBuf = chunkData.buffer;
          chunksAwaitingResolution.add(chunkData);
        } else {
          chunkData.resolveChunk(new ClosedChannelException());
        }
      } finally {
        lock.unlock();
      }
    }
    return chunkBuf;
  }

  /**
   * Resolves all the remaining chunks with {@link ClosedChannelException}.
   * @param e the exception to use to resolve all the chunks.
   */
  private void resolveAllRemainingChunks(Exception e) {
    while (chunksAwaitingResolution.peek() != null) {
      chunksAwaitingResolution.poll().resolveChunk(e);
    }
    while (chunks.peek() != null) {
      chunks.poll().resolveChunk(e);
    }
  }
}

/**
 * Representation of all the data associated with a chunk i.e. the actual bytes and the future and callback that need to
 * be invoked on resolution.
 */
class ChunkData {
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
    long bytesWritten = buffer.position() - startPos;
    future.done(bytesWritten, exception);
    if (callback != null) {
      callback.onCompletion(bytesWritten, exception);
    }
  }
}
