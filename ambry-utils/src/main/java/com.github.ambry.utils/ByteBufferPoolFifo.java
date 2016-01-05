package com.github.ambry.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


/**
 * An implementation of {@link ByteBufferPool}. When the memory in the pool is not enough,
 * further requests will be blocked before respectively goes timeout. The blocked requests
 * are queued and served in the manner of "first come, first serve". This implementation
 * only ensures that the available memory in the pool never goes above the initially-set
 * pool capacity or below zero. It does not actually "pool" deallocated buffers.
 */
public class ByteBufferPoolFifo implements ByteBufferPool {
  private final long capacity;
  private final ReentrantLock lock;
  private final Deque<Condition> waiters;
  private long availableMemory;

  /**
   * Create a new buffer pool
   * @param capacity the maximum amount of memory that this buffer pool can
   *                 allocate
   */
  public ByteBufferPoolFifo(long capacity) {
    this.lock = new ReentrantLock();
    this.waiters = new ArrayDeque<Condition>();
    this.capacity = capacity;
    this.availableMemory = capacity;
  }

  @Override
  public ByteBuffer allocate(int size, long timeToBlockMs)
      throws IOException, TimeoutException, InterruptedException {
    if (size > capacity) {
      throw new IOException("Requested size cannot exceed pool capacity.");
    }
    lock.lock();
    try {
      if (availableMemory >= size) {
        availableMemory -= size;
        return ByteBuffer.allocate(size);
      } else {
        ByteBuffer buffer = null;
        Condition enoughMemory = lock.newCondition();
        waiters.addLast(enoughMemory);
        while (size > availableMemory) {
          if (!enoughMemory.await(timeToBlockMs, TimeUnit.MILLISECONDS)) {
            waiters.removeFirst();
            throw new TimeoutException("Memory not Enough. Request timeout.");
          }
        }
        Condition removed = waiters.removeFirst();
        if (removed != enoughMemory) {
          throw new IllegalStateException("Wrong condition: this shouldn't happen.");
        }
        availableMemory -= size;
        if (availableMemory > 0) {
          if (!waiters.isEmpty()) {
            waiters.peekFirst().signal();
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

  @Override
  public void deallocate(ByteBuffer buffer) throws IOException {
    lock.lock();
    try {
      if(availableMemory+buffer.capacity()>capacity){
        throw new IOException("Total buffer size cannot exceed pool capacity");
      }
      availableMemory += buffer.capacity();
      if (!waiters.isEmpty()) {
        waiters.peekFirst().signal();
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public long availableMemory() {
    lock.lock();
    try {
      return availableMemory;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public long capacity() {
    return capacity;
  }
}

