/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.utils;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * <p>
 * An implementation of <B>bounded</B> {@link ByteBufferPool}. A pool
 * is bounded by non-varying {@code capacity}, and it ensures that the
 * available memory in the pool never goes above this capacity, or goes
 * below zero. It does not actually "pool" deallocated buffers.
 */
public class SimpleByteBufferPool implements ByteBufferPool {
  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final long capacity;
  private final Object lock;
  private long availableMemory;

  /**
   * Create a new buffer pool
   * @param capacity the maximum amount of memory that this buffer pool can
   *                 allocate. The caller has the responsibility to assign
   *                 a reasonable value for {@code capacity}.
   */
  public SimpleByteBufferPool(long capacity) {
    this.lock = new Object();
    this.capacity = capacity;
    this.availableMemory = capacity;
  }

  /**
   * Allocate a byte buffer at the requested size
   * @param size the buffer size to allocate in bytes
   * @param timeToBlockInMs a non-negative maximum time in milliseconds to block a request
   *                        until the requested size of memory becomes available. Zero value
   *                        will make the pool to try a single time, either return a {@link
   *                        ByteBuffer} if memory is available, or a {@code TimeoutException}
   * @return A {@link ByteBuffer} at the requested size
   * @throws TimeoutException if request cannot be served within {@code timeToBlockInMs}
   * @throws InterruptedException if the current thread is interrupted while waiting
   * @throws IllegalArgumentException if {@code size} is larger than the pool capacity, or
   *                                  if {@code timeToBlockInMs} is negative.
   */
  @Override
  public ByteBuffer allocate(int size, final long timeToBlockInMs) throws TimeoutException, InterruptedException {
    if (size > capacity) {
      throw new IllegalArgumentException("Requested size cannot exceed pool capacity.");
    } else if (timeToBlockInMs < 0) {
      throw new IllegalArgumentException("timeToBlockInMs cannot be negative.");
    }
    final long startTimeInMs = System.currentTimeMillis();
    synchronized (lock) {
      while (size > availableMemory) {
        long timeout = timeToBlockInMs - (System.currentTimeMillis() - startTimeInMs);
        if (timeout <= 0) {
          throw new TimeoutException("Timed out waiting for allocation.");
        }
        lock.wait(timeout);
      }
      availableMemory -= size;
      if (availableMemory > 0) {
        lock.notify();
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
        logger.warn("The total deallocated memory is more than that had been allocated from the buffer pool.");
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

