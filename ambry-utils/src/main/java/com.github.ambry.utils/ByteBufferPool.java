package com.github.ambry.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeoutException;


/**
 * A buffer pool interface that defines a utility for allocating (checkout from
 * the pool) and deallocating (checkin to the pool) {@link ByteBuffer}. A buffer
 * pool ensures that at any time, the total size of allocated buffers does not
 * exceed the pool capacity, and the available memory in the pool does not exceed
 * the pool capacity.
 */
public interface ByteBufferPool {
  /**
   * Allocate a byte buffer at the requested size
   * @param size the buffer size to allocate in bytes
   * @param timeToBlockMs the maximum time in milliseconds to block a request
   *                      until mor memory becomes available
   * @return A byte buffer at requested size
   * @throws IOException if requested size exceeds the pool capacity
   * @throws TimeoutException if request cannot be served within {@code timeToBlockMs}
   * @throws InterruptedException if the current thread is interrupted
   *         while waiting
   */
  ByteBuffer allocate(int size, long timeToBlockMs)
      throws IOException, TimeoutException, InterruptedException;

  /**
   * Reclaim the buffer to the pool.
   * @param buffer the byte buffer to be deallocated
   * @throws IOException if the pool exceeds its capacity after reclaim the buffer
   */
  void deallocate(ByteBuffer buffer)
      throws IOException;

  /**
   * @return The memory size currently available in the pool.
   */
  long availableMemory();

  /**
   * @return The total memory managed by this pool
   */
  long capacity();
}
