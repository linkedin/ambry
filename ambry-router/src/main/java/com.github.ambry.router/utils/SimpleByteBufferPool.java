package com.github.ambry.router.utils;

import com.github.ambry.router.RouterErrorCode;
import com.github.ambry.router.RouterException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


/**
 * A simple {@link ByteBufferPool} implimentation that does not actually
 * "pool" deallocated buffers. Instead, it only tracks memory usage in the
 * pool, and ensures that the total allocated buffers will not exceed the
 * maximum allowrance.
 */
public class SimpleByteBufferPool implements ByteBufferPool {
  private final long capacity;
  private final ReentrantLock lock;
  private final Deque<Condition> waiters;
  private long availableMemory;

  /**
   * Create a new buffer pool
   * @param capacity The maximum amount of memory that this buffer pool can
   *                 allocate
   */
  public SimpleByteBufferPool(long capacity) {
    this.lock = new ReentrantLock();
    this.waiters = new ArrayDeque<Condition>();
    this.capacity = capacity;
    this.availableMemory = capacity;
  }

  @Override
  public ByteBuffer allocate(int size, long timeToBlockMs)
      throws InterruptedException, RouterException {
    if (size > this.capacity) {
      throw new RouterException("Buffer cannot be larger than "
          + this.capacity, RouterErrorCode.ExceedPoolCapacity);
    }
    this.lock.lock();
    try {
      if (this.availableMemory >= size) {
        this.availableMemory -= size;
        lock.unlock();
        return ByteBuffer.allocate(size);
      } else {
        ByteBuffer buffer = null;
        Condition enoughMemory = this.lock.newCondition();
        this.waiters.addLast(enoughMemory);
        while (size > this.availableMemory) {
          if (!enoughMemory.await(timeToBlockMs, TimeUnit.MILLISECONDS)) {
            throw new RouterException("Request timeout.",
                RouterErrorCode.BufferPoolNotEnoughMemory);
          }
        }
        Condition removed = this.waiters.removeFirst();
        if (removed != enoughMemory) {
          throw new IllegalStateException("Wrong condition: this shouldn't happen.");
        }
        this.availableMemory -= size;
        if (this.availableMemory > 0) {
          if (!this.waiters.isEmpty()) {
            this.waiters.peekFirst().signal();
          }
        }
        lock.unlock();
        if (buffer == null) {
          return ByteBuffer.allocate(size);
        } else {
          return buffer;
        }
      }
    } finally {
      if (lock.isHeldByCurrentThread()) {
        lock.unlock();
      }
    }
  }

  public void deallocate(ByteBuffer buffer, int size) {
    lock.lock();
    try {
      this.availableMemory += size;
      if (!this.waiters.isEmpty()) {
        this.waiters.peekFirst().signal();
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void deallocate(ByteBuffer buffer) {
    deallocate(buffer, buffer.capacity());
  }

  @Override
  public long availableMemory() {
    lock.lock();
    try {
      return this.availableMemory;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public int queued() {
    lock.lock();
    try {
      return this.waiters.size();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public long capacity() {
    return this.capacity;
  }
}

