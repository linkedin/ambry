package com.github.ambry.router;

import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * TODO: write description
 */
public class ByteBufferSWC implements ScheduledWriteChannel {
  private final AtomicBoolean channelOpen = new AtomicBoolean(true);
  private final LinkedBlockingQueue<ChunkData> chunks = new LinkedBlockingQueue<ChunkData>();
  private final Map<ByteBuffer, ChunkData> chunksAwaitingResolution = new LinkedHashMap<ByteBuffer, ChunkData>();

  @Override
  public Future<Long> write(ByteBuffer src, Callback<Long> callback) {
    if(src == null) {
      throw new IllegalArgumentException("Source buffer cannot be null");
    }
    ChunkData chunkData = new ChunkData(src, callback) ;
    if (!isOpen()) {
      chunkData.resolveChunk(0, new ClosedChannelException());
    } else {
      chunks.add(chunkData);
    }
    return chunkData.future;
  }

  @Override
  public boolean isOpen() {
    return channelOpen.get();
  }

  @Override
  public void close() {
    if(channelOpen.compareAndSet(true, false)) {
      resolveAllRemainingChunks();
      chunks.add(new ChunkData(null, null));
    }
  }

  public ByteBuffer getNextChunk() {
    ByteBuffer chunk = null;
    if(isOpen()) {
      try {
        ChunkData chunkData = chunks.take();
        if (chunkData.buffer != null) {
          chunk = chunkData.buffer;
          chunksAwaitingResolution.put(chunk, chunkData);
        }
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
    }
    return chunk;
  }

  public void resolveChunk(ByteBuffer chunk, Exception exception) {
    if(isOpen()) {
      int bytesRead = chunk.position();
      chunk.clear();
      if (!chunksAwaitingResolution.containsKey(chunk)) {
        throw new IllegalArgumentException("Unrecognized chunk");
      }
      chunksAwaitingResolution.remove(chunk).resolveChunk(bytesRead, exception);
    }
  }

  private void resolveAllRemainingChunks() {
    Iterator<Map.Entry<ByteBuffer, ChunkData>> chunkIterator = chunksAwaitingResolution.entrySet().iterator();
    while (chunkIterator.hasNext()) {
      Map.Entry<ByteBuffer, ChunkData> chunkInfo = chunkIterator.next();
      // best guess.
      int bytesRead = chunkInfo.getKey().position();
      chunkInfo.getValue().resolveChunk(bytesRead, new ClosedChannelException());
      chunkIterator.remove();
    }
    ChunkData chunkData = chunks.poll();
    while (chunkData != null) {
      chunkData.resolveChunk(0, new ClosedChannelException());
      chunkData = chunks.poll();
    }
  }
}

class ChunkData {
  public final FutureResult<Long> future = new FutureResult<Long>();
  public final ByteBuffer buffer;


  private final Callback<Long> callback;

  public ChunkData(ByteBuffer buffer, Callback<Long> callback) {
    this.buffer = buffer;
    this.callback = callback;
  }

  public void resolveChunk(long bytesWritten, Exception exception) {
    IllegalStateException ise = null;
    if (exception != null) {
      ise = new IllegalStateException(exception);
    }
    future.done(bytesWritten, ise);
    if(callback != null) {
      callback.onCompletion(bytesWritten, exception);
    }
  }
}
