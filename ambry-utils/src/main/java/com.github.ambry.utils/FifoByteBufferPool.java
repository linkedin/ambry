package com.github.ambry.utils;

import java.nio.ByteBuffer;
import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


/**
 * <p>
 * An implementation of <B>bounded</B> {@link ByteBufferPool}. A pool
 * is bounded by non-varying {@code capacity}, and it ensures that the
 * available memory in the pool never goes above this capacity, or goes
 * below zero. It does not actually "pool" deallocated buffers.
 * <p>
 * This implementation maintains an explicit FIFO queue for all the
 * blocked requests. However, a new arriving request (i.e., not in the
 * queue) may preempt over the blocked requests.
 */
public class FifoByteBufferPool implements ByteBufferPool {
  private final long capacity;
  private final ReentrantLock lock;
  private final Deque<Condition> waiters;
  private long availableMemory;

  /**
   * Create a new buffer pool
   * @param capacity the maximum amount of memory that this buffer pool can
   *                 allocate
   */
  public FifoByteBufferPool(long capacity) {
    this.lock = new ReentrantLock();
    this.waiters = new LinkedList<Condition>();
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
  public ByteBuffer allocate(int size, long timeToBlockInMs)
      throws TimeoutException, InterruptedException {
    if (size > capacity) {
      throw new IllegalArgumentException("Requested size cannot exceed pool capacity.");
    }
    final long startTimeInMs = System.currentTimeMillis();
    lock.lock();
    try {
      if (availableMemory >= size) {
        availableMemory -= size;
        return ByteBuffer.allocate(size);
      } else {
        Condition enoughMemory = lock.newCondition();
        waiters.addLast(enoughMemory);
        while (size > availableMemory) {
          if (!enoughMemory
              .await(timeToBlockInMs - (System.currentTimeMillis() - startTimeInMs), TimeUnit.MILLISECONDS)) {
            waiters.removeFirst();
            throw new TimeoutException("Memory not Enough. Request timeout.");
          }
        }
        Condition removed = waiters.removeFirst();
        availableMemory -= size;
        if (availableMemory > 0 && !waiters.isEmpty()) {
          waiters.peekFirst().signal();
        }
        return ByteBuffer.allocate(size);
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * This method claims back the capacity of {@code buffer}. It does not
   * check if the buffer was originally allocated from the pool. If a
   * deallocation will exceed the pool's capacity, the method simply sets
   * the pool's available memory to its {@code capacity}.
   * @param buffer the byte buffer to be deallocated back to the pool
   */
  @Override
  public void deallocate(ByteBuffer buffer) {
    lock.lock();
    try {
      availableMemory += buffer.capacity();
      if (availableMemory > capacity) {
        availableMemory = capacity;
      }
      if (!waiters.isEmpty()) {
        waiters.peekFirst().signal();
      }
    } finally {
      lock.unlock();
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

