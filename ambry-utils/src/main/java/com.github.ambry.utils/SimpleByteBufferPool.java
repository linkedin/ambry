package com.github.ambry.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeoutException;


/**
 * A simple {@link ByteBufferPool} implementation. It only ensures that the
 * available memory in the pool never goes above the initially-set pool
 * capacity or below zero. It does not actually "pool" deallocated buffers.
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

  @Override
  public ByteBuffer allocate(int size, long timeToBlockInMs)
      throws IOException, TimeoutException, InterruptedException {
    if (size > capacity) {
      throw new IOException("Requested size cannot exceed pool capacity.");
    }
    ThreadLocal<Long> startTimeInMs = new ThreadLocal<Long>();
    startTimeInMs.set((System.currentTimeMillis()));
    ThreadLocal<Long> timeout = new ThreadLocal<Long>();
    timeout.set(timeToBlockInMs);
    synchronized (lock) {
      while (size > availableMemory) {
        timeout.set(timeToBlockInMs - (System.currentTimeMillis() - startTimeInMs.get()));
        if (timeout.get() <= 0) {
          throw new TimeoutException("Memory not Enough. Request timeout.");
        }
        lock.wait(timeToBlockInMs - (System.currentTimeMillis() - startTimeInMs.get()));
      }
      availableMemory -= size;
      if (availableMemory > 0) {
        lock.notifyAll();
      }
    }
    return ByteBuffer.allocate(size);
  }

  @Override
  public void deallocate(ByteBuffer buffer)
      throws IOException {
    synchronized (lock) {
      if (availableMemory + buffer.capacity() > capacity) {
        throw new IOException("Total buffer size cannot exceed pool capacity");
      }
      availableMemory += buffer.capacity();
      lock.notifyAll();
    }
  }

  @Override
  public long availableMemory() {
    return availableMemory;
  }

  @Override
  public long capacity() {
    return capacity;
  }
}

