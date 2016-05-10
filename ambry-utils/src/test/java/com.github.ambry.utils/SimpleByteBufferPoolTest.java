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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * This class tests bounded {@link ByteBufferPool} implementation.
 */
public class SimpleByteBufferPoolTest {

  /**
   * Scenario: simple non-blocking allocation and deallocation.
   */
  @Test
  public void testSingleRequestAllocateDeallocate()
      throws TimeoutException, InterruptedException {
    final long capacity = 2 * 1024;
    final int size = 1024;
    long maxBlockTimeInMs = 200;
    SimpleByteBufferPool pool = new SimpleByteBufferPool(capacity);
    assertEquals(capacity, pool.availableMemory());
    ByteBuffer buffer = pool.allocate(size, maxBlockTimeInMs);
    assertEquals(size, buffer.capacity());
    assertEquals(capacity - size, pool.availableMemory());
    pool.deallocate(buffer);
    assertEquals(capacity, pool.availableMemory());
  }

  /**
   * Scenario: requested size is larger than pool capacity.
   */
  @Test
  public void testRequestExceedPoolCapacity()
      throws TimeoutException, InterruptedException {
    final long capacity = 1024;
    final long maxBlockTimeInMs = 200;
    SimpleByteBufferPool pool = new SimpleByteBufferPool(capacity);
    try {
      ByteBuffer buffer = pool.allocate((int) capacity + 1, maxBlockTimeInMs);
      fail("Should have thrown!");
    } catch (IllegalArgumentException e) {
    }
  }

  /**
   * Scenario: buffer pool exceeds its capacity after reclaiming buffers.
   */
  @Test
  public void testDeallocateExceedPoolCapacity()
      throws TimeoutException, InterruptedException {
    final int capacity = 1024;
    SimpleByteBufferPool pool = new SimpleByteBufferPool(capacity);
    ByteBuffer singleBuffer = ByteBuffer.allocate(1);
    pool.deallocate(singleBuffer);
    assertEquals(capacity, pool.availableMemory());
  }

  /**
   * Scenario: timeout after not enough memory.
   */
  @Test
  public void testNotEnoughMemory()
      throws InterruptedException, TimeoutException {
    final int size = 1024;
    final long capacity = 1024;
    final long maxBlockTimeInMs = 20;
    SimpleByteBufferPool pool = new SimpleByteBufferPool(capacity);
    ByteBuffer buffer1 = pool.allocate(size, maxBlockTimeInMs);
    try {
      ByteBuffer buffer2 = pool.allocate(size, maxBlockTimeInMs);
      fail("should have thrown.");
    } catch (TimeoutException e) {
    }
  }

  /**
   * Scenario: zero and negative timeout.
   */
  @Test
  public void testNegativeBlockTime()
      throws InterruptedException, TimeoutException {
    final int size = 1024;
    final long capacity = 1024;
    final long maxBlockTimeInMs = 20;
    SimpleByteBufferPool pool = new SimpleByteBufferPool(capacity);
    ByteBuffer buffer=pool.allocate(size, 0);
    pool.deallocate(buffer);
    try{
      buffer=pool.allocate(size, -maxBlockTimeInMs);
    }catch(IllegalArgumentException e){
    }
    pool.deallocate(buffer);
    assertEquals(size, pool.capacity());
  }

  /**
   * Scenario: a request is first blocked, and then served before timeout.
   */
  @Test
  public void testFirstBlockedThenSucceed()
      throws Exception {
    final int size = 1024;
    final long capacity = 1024;
    final long maxBlockTimeInMs = 200;
    SimpleByteBufferPool pool = new SimpleByteBufferPool(capacity);
    CountDownLatch[] allocated = new CountDownLatch[2];
    CountDownLatch[] used = new CountDownLatch[2];
    BufferConsumer[] consumers = new BufferConsumer[2];
    Thread[] consumerThreads = new Thread[2];
    for (int i = 0; i < 2; i++) {
      allocated[i] = new CountDownLatch(1);
      used[i] = new CountDownLatch(1);
      consumers[i] = new BufferConsumer(size, maxBlockTimeInMs, pool, allocated[i], used[i]);
      consumerThreads[i] = new Thread(consumers[i]);
      consumerThreads[i].start();
    }
    allocated[0].await();
    assertEquals(0, pool.availableMemory());
    for (int i = 0; i < 2; i++) {
      used[i].countDown();
      consumerThreads[i].join();
      if (consumers[i].exception != null) {
        throw consumers[i].exception;
      }
    }
    assertEquals(capacity, pool.availableMemory());
  }

  /**
   * Scenario: n requests are first blocked, and then these requests are served after
   * a large buffer is released back to the pool.
   */
  @Test
  public void testMultipleRequestedServedAfterBlocked()
      throws Exception {
    final int n = 3;
    final int smallSize = 1024;
    final int largeSize = n * smallSize;
    final long capacity = n * smallSize;
    final long maxBlockTimeInMs = 200;
    SimpleByteBufferPool pool = new SimpleByteBufferPool(capacity);
    CountDownLatch largeAllocated = new CountDownLatch(1);
    CountDownLatch largeUsed = new CountDownLatch(1);
    CountDownLatch smallAllocated = new CountDownLatch(n);
    CountDownLatch smallUsed = new CountDownLatch(n);
    BufferConsumer largeConsumer = new BufferConsumer(largeSize, maxBlockTimeInMs, pool, largeAllocated, largeUsed);
    new Thread(largeConsumer).start();
    largeAllocated.await();
    assertEquals(0, pool.availableMemory());
    BufferConsumer[] smallConsumers = new BufferConsumer[n];
    Thread[] smallConsumerThreads = new Thread[n];
    for (int i = 0; i < n; i++) {
      smallConsumers[i] = new BufferConsumer(smallSize, maxBlockTimeInMs, pool, smallAllocated, smallUsed);
      smallConsumerThreads[i] = new Thread(smallConsumers[i]);
      smallConsumerThreads[i].start();
    }
    largeUsed.countDown();
    smallAllocated.await();
    assertEquals(0, pool.availableMemory());
    for (int i = 0; i < n; i++) {
      smallUsed.countDown();
    }
    for (int i = 0; i < n; i++) {
      smallConsumerThreads[i].join();
    }
    assertEquals(capacity, pool.availableMemory());
    if (largeConsumer.exception != null) {
      throw largeConsumer.exception;
    }
    for (int i = 0; i < n; i++) {
      if (smallConsumers[i].exception != null) {
        throw smallConsumers[i].exception;
      }
    }
  }

  /**
   * Scenario: three requests R0, R1, and R2.
   * t0: R0 served, R1 and R2 blocked.
   * t1: R1 timeout.
   * t2: R0 deallocated and R2 served.
   */
  @Test
  public void testOneExpiredAnotherServed()
      throws Exception {
    final int size = 1024;
    final long capacity = 1024;
    final int numOfRequests = 3;
    final long[] blockTimeInMs = {10, 10, 1000};
    SimpleByteBufferPool pool = new SimpleByteBufferPool(capacity);
    CountDownLatch[] allocated = new CountDownLatch[numOfRequests];
    CountDownLatch[] used = new CountDownLatch[numOfRequests];
    BufferConsumer[] consumers = new BufferConsumer[numOfRequests];
    Thread[] consumerThreads = new Thread[numOfRequests];
    for (int i = 0; i < numOfRequests; i++) {
      allocated[i] = new CountDownLatch(1);
      used[i] = new CountDownLatch(1);
      consumers[i] = new BufferConsumer(size, blockTimeInMs[i], pool, allocated[i], used[i]);
      consumerThreads[i] = new Thread(consumers[i]);
      consumerThreads[i].start();
    }
    consumerThreads[1].join(); //wait until consumers[1] timeout
    if (!(consumers[1].exception instanceof TimeoutException)) {
      throw consumers[1].exception;
    }
    used[0].countDown();
    used[2].countDown();
    consumerThreads[0].join();
    consumerThreads[2].join();
    if (consumers[0].exception != null) {
      throw consumers[0].exception;
    } else if (consumers[2].exception != null) {
      throw consumers[2].exception;
    }
    assertEquals(capacity, pool.availableMemory());
  }

  private class BufferConsumer implements Runnable {
    final int size;
    final long maxBlockTimeInMs;
    final ByteBufferPool pool;
    final CountDownLatch allocated;
    final CountDownLatch used;
    ByteBuffer buffer;
    public Exception exception = null;

    private BufferConsumer(int size, long maxBlockTimeInMs, ByteBufferPool pool, CountDownLatch allocated,
        CountDownLatch used) {
      this.size = size;
      this.maxBlockTimeInMs = maxBlockTimeInMs;
      this.pool = pool;
      this.allocated = allocated;
      this.used = used;
    }

    @Override
    public void run() {
      try {
        buffer = pool.allocate(size, maxBlockTimeInMs);
        allocated.countDown();
        used.await();
        pool.deallocate(buffer);
      } catch (Exception e) {
        exception = e;
      }
    }
  }
}
