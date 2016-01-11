package com.github.ambry.utils;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeoutException;


/**
 * <p>
 * An implementation of <B>bounded</B> {@link ByteBufferPool}. A pool
 * is bounded by non-varying {@code capacity}, and it ensures that the
 * available memory in the pool never goes above this capacity, or goes
 * below zero. It does not actually "pool" deallocated buffers.
 */
public class SimpleByteBufferPool implements ByteBufferPool {
  private final long capacity;
  private final Object lock;
  private long availableMemory;

  /**
   * Create a new buffer pool
   * @param capacity the maximum amount of memory that this buffer pool can
   *                 allocate
   */
  public SimpleByteBufferPool(long capacity) {
    this.lock = new Object();
    this.capacity = capacity;
    this.availableMemory = capacity;
  }

  /**
   * Allocate a byte buffer at the requested size
   * @param size the buffer size to allocate in bytes
   * @param timeToBlockInMs the maximum time in milliseconds to block a request
   *                      until the requested size of memory becomes available
   * @return A {@link ByteBuffer} at the requested size
   * @throws TimeoutException if request cannot be served within {@code timeToBlockMs}
   * @throws InterruptedException if the current thread is interrupted while waiting
   * @throws IllegalArgumentException if {@code size} is larger than the pool capacity
   */
  @Override
  public ByteBuffer allocate(int size, final long timeToBlockInMs)
      throws TimeoutException, InterruptedException {
    if (size > capacity) {
      throw new IllegalArgumentException("Requested size cannot exceed pool capacity.");
    }
    final long startTimeInMs = System.currentTimeMillis();
    long timeout = timeToBlockInMs;
    synchronized (lock) {
      while (size > availableMemory) {
        timeout = timeToBlockInMs - (System.currentTimeMillis() - startTimeInMs);
        if (timeout <= 0) {
          throw new TimeoutException("Memory not Enough. Request timeout.");
        }
        lock.wait(timeout);
      }
      availableMemory -= size;
      if (availableMemory > 0) {
        lock.notifyAll();
      }
    }
    return ByteBuffer.allocate(size);
  }

  /**
   * This method claims back the memory of {@code buffer}. It does not
   * check if the buffer was originally allocated from the pool. If a
   * deallocation will exceed the pool's capacity, the method simply sets
   * the pool's available memory to its {@code capacity}.
   * @param buffer the {@link ByteBuffer} to be deallocated back to the pool
   */
  @Override
  public void deallocate(ByteBuffer buffer) {
    synchronized (lock) {
      availableMemory += buffer.capacity();
      if (availableMemory > capacity) {
        availableMemory = capacity;
      }
      lock.notify();
    }
  }

  /**
   * @return the amount of memory currently available
   */
  public long availableMemory() {
    return availableMemory;
  }

  /**
   * @return the capacity of the pool
   */
  public long capacity() {
    return capacity;
  }
}

