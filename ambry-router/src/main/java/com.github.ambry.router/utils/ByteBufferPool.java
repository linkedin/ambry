package com.github.ambry.router.utils;

import com.github.ambry.router.RouterException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeoutException;


/**
 * The interface for a ByteBuffer pool. A request that cannot be served
 * immediately due to memory unavaiability will block until a timeout
 * exception is thrown.
 */
public interface ByteBufferPool {
  /**
   * Allocate a byte buffer at the requested size
   * @param size The buffer size to allocate in bytes
   * @param timeToBlockMs The maximum time in milliseconds to block request
   *                      until more memory becomes available
   * @return The byte buffer
   * @throws InterruptedException
   * @throws RouterException
   */
  ByteBuffer allocate(int size, long timeToBlockMs)
      throws InterruptedException, RouterException;

  /**
   * Return buffers to the pool by reclaiming the size of this buffer.
   * @param buffer
   */
  void deallocate(ByteBuffer buffer);

  /**
   * The memory size currently available in the pool.
   * @return
   */
  long availableMemory();

  /**
   * The number of requests being blocked
   * @return
   */
  int queued();

  /**
   * The total memory managed by this pool
   * @return
   */
  long capacity();
}
