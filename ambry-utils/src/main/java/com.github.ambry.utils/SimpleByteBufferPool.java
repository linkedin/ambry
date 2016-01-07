package com.github.ambry.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;


/**
 * A simple {@link ByteBufferPool} implementation. It only ensures that the
 * available memory in the pool never goes above the initially-set pool
 * capacity or below zero. It does not actually "pool" deallocated buffers.
 */
public class SimpleByteBufferPool implements ByteBufferPool {
  private final long capacity;
  private final Object lock;
  private AtomicLong availableMemory;

  /**
   * Create a new buffer pool
   * @param capacity the maximum amount of memory that this buffer pool can
   *                 allocate
   */
  public SimpleByteBufferPool(long capacity) {
    this.lock = new Object();
    this.capacity = capacity;
    this.availableMemory = new AtomicLong(capacity);
  }

  @Override
  public ByteBuffer allocate(int size, long timeToBlockMs)
      throws IOException, TimeoutException, InterruptedException {
    ThreadLocal<Long> startTimeMs = new ThreadLocal<Long>();
    startTimeMs.set((System.currentTimeMillis()));
    if (size > capacity) {
      throw new IOException("Requested size cannot exceed pool capacity.");
    }
    long oldVal;
    long newVal;
    while (true) {
      oldVal = availableMemory.get();
      if (oldVal >= size) {
        newVal = oldVal - size;
        if (availableMemory.compareAndSet(oldVal, newVal)) {
          if (System.currentTimeMillis() >= startTimeMs.get() + timeToBlockMs) {
            throw new TimeoutException("Memory not Enough. Request timeout.");
          }
          break;
        }
      } else {
        synchronized (lock) {
          lock.wait(timeToBlockMs);
        }
        if (System.currentTimeMillis() >= startTimeMs.get() + timeToBlockMs) {
          throw new TimeoutException("Memory not Enough. Request timeout.");
        }
      }
    }
    if (availableMemory.get() > 0) {
      synchronized (lock) {
        lock.notifyAll();
      }
    }
    return ByteBuffer.allocate(size);
  }

  @Override
  public void deallocate(ByteBuffer buffer) throws IOException {
    long oldVal;
    long newVal;
    while (true) {
      oldVal = availableMemory.get();
      newVal = oldVal + buffer.capacity();
      if(newVal>capacity){
        throw new IOException("Total buffer size cannot exceed pool capacity");
      }
      if (availableMemory.compareAndSet(oldVal, newVal)) {
        break;
      }
    }
    synchronized (lock) {
      lock.notifyAll();
    }
  }

  @Override
  public long availableMemory() {
    return availableMemory.get();
  }

  @Override
  public long capacity() {
    return capacity;
  }
}

