package com.github.ambry.utils;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeoutException;


/**
 * A buffer pool interface that defines a utility for allocating and deallocating
 * (reclaiming back to the pool) {@link ByteBuffer}.
 */
public interface ByteBufferPool {
  /**
   * Allocate a byte buffer at the requested size
   * @param size the buffer size to allocate in bytes
   * @param timeToBlockInMs the maximum time in milliseconds to block a request
   *                      until the requested size of memory becomes available
   * @return A {@link ByteBuffer} at the requested size
   * @throws TimeoutException if request cannot be served within {@code timeToBlockMs}
   * @throws InterruptedException if the current thread is interrupted while waiting
   */
  ByteBuffer allocate(int size, long timeToBlockInMs)
      throws TimeoutException, InterruptedException;

  /**
   * Reclaim a {@link ByteBuffer} to the pool.
   * @param buffer the {@link ByteBuffer} to be deallocated back to the pool
   */
  void deallocate(ByteBuffer buffer);
}
