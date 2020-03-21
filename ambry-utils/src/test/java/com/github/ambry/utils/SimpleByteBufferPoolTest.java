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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * This class tests {@link SimpleByteBufferPool} that implements {@link ByteBufferPool}.
 */
public class SimpleByteBufferPoolTest {

  /**
   * This is a simple scenario where the {@link SimpleByteBufferPool} has enough buffer to allocate a certain amount
   * of {@link ByteBuffer}, and then deallocated the same amount of {@link ByteBuffer} back to the buffer pool. No
   * error should occur during this test.
   */
  @Test
  public void testSingleRequestAllocateDeallocate() throws Exception {
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
   * This scenario tests when a {@link SimpleByteBufferPool} tries to allocate a {@link ByteBuffer} that is larger
   * than its capacity. This operation should result in an {@link IllegalArgumentException}.
   */
  @Test
  public void testRequestExceedPoolCapacity() throws Exception {
    final long capacity = 1024;
    final long maxBlockTimeInMs = 200;
    SimpleByteBufferPool pool = new SimpleByteBufferPool(capacity);
    try {
      pool.allocate((int) capacity + 1, maxBlockTimeInMs);
      fail("Should have thrown!");
    } catch (IllegalArgumentException e) {
    }
  }

  /**
   * This scenario tests when a {@link SimpleByteBufferPool} deallocates a {@link ByteBuffer} that will make the
   * pool's total available memory to be larger than the pool's capacity. This operation should still suceed, and
   * set the pool's available memory the same as its capacity.
   */
  @Test
  public void testDeallocateExceedPoolCapacity() throws Exception {
    final int capacity = 1024;
    SimpleByteBufferPool pool = new SimpleByteBufferPool(capacity);
    ByteBuffer singleBuffer = ByteBuffer.allocate(1);
    pool.deallocate(singleBuffer);
    assertEquals(capacity, pool.availableMemory());
  }

  /**
   * This scenario tests when a {@link SimpleByteBufferPool} tries to allocate a {@link ByteBuffer} that is smaller
   * than the pool's capacity, but larger than the pool's current available memory. The operation will be blocking
   * until eventually be timed out and throw {@link TimeoutException}.
   */
  @Test
  public void testNotEnoughMemory() throws Exception {
    final int size = 1024;
    final long capacity = 1024;
    final long maxBlockTimeInMs = 10;
    SimpleByteBufferPool pool = new SimpleByteBufferPool(capacity);
    pool.allocate(size, maxBlockTimeInMs);
    try {
      pool.allocate(size, maxBlockTimeInMs);
      fail("should have thrown.");
    } catch (TimeoutException e) {
    }
  }

  /**
   * This test ensures that the {@link SimpleByteBufferPool} behaves correctly when the {@code timeToBlockInMs}
   * parameter set in {@link SimpleByteBufferPool#allocate(int, long)} is zero or a negative value.
   */
  @Test
  public void testNegativeBlockTime() throws Exception {
    final int size = 1024;
    final long capacity = 1024;
    final long maxBlockTimeInMs = 20;
    SimpleByteBufferPool pool = new SimpleByteBufferPool(capacity);
    ByteBuffer buffer = pool.allocate(size, 0);
    pool.deallocate(buffer);
    try {
      pool.allocate(size, -maxBlockTimeInMs);
      fail("IllegalArgumentException should have been thrown when timeToBlockInMs is negative.");
    } catch (IllegalArgumentException e) {
    }
    try {
      pool.allocate((int) capacity + 1, maxBlockTimeInMs);
      fail(
          "IllegalArgumentException should have been thrown when requested buffer size is larger than the buffer pool's capacity.");
    } catch (IllegalArgumentException e) {
    }
    assertEquals(size, pool.capacity());
  }

  /**
   * This scenario tests when a {@link SimpleByteBufferPool} tries to allocate a certain amount of memory in several
   * stages. First, the requested amount of memory is not availeble, so the allocation operation is blocking. Before
   * the operation is timed out, the requested amount of memory becomes available, and the allocation becomes successfully.
   * No exception should occur during this test.
   */
  @Test
  public void testFirstBlockedThenSucceed() throws Exception {
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
    }
    consumerThreads[0].start();
    if (!allocated[0].await(100, TimeUnit.MILLISECONDS)) {
      fail("SimpleByteBufferPool takes too long time to allocate a buffer to BufferConsumer[0].");
    }
    assertEquals(0, pool.availableMemory());
    consumerThreads[1].start();
    assertEquals("BufferConsumer-1 should not have got buffer from the buffer pool.", 1, allocated[1].getCount());
    used[0].countDown();
    consumerThreads[0].join();
    used[1].countDown();
    consumerThreads[1].join();
    for (int i = 0; i < 2; i++) {
      if (consumers[i].exception != null) {
        throw consumers[i].exception;
      }
    }
    assertEquals(capacity, pool.availableMemory());
  }

  /**
   * This scenario tests when there is not enough memory, n requests are blocked during getting memory from a
   * {@link SimpleByteBufferPool}. After a large amount of buffer is deallocated back to the buffer pool, all
   * the n requests can be served successfully. There should be no exception during the test.
   */
  @Test
  public void testMultipleRequestedServedAfterBlocked() throws Exception {
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
    if (!largeAllocated.await(100, TimeUnit.MILLISECONDS)) {
      fail("SimpleByteBufferPool takes too long time to allocate a buffer to largeConsumer.");
    }
    assertEquals(0, pool.availableMemory());
    BufferConsumer[] smallConsumers = new BufferConsumer[n];
    Thread[] smallConsumerThreads = new Thread[n];
    for (int i = 0; i < n; i++) {
      smallConsumers[i] = new BufferConsumer(smallSize, maxBlockTimeInMs, pool, smallAllocated, smallUsed);
      smallConsumerThreads[i] = new Thread(smallConsumers[i]);
      smallConsumerThreads[i].start();
    }
    largeUsed.countDown();
    if (!smallAllocated.await(100, TimeUnit.MILLISECONDS)) {
      fail("SimpleByteBufferPool takes too long time to allocate a buffer to smallConsumer.");
    }
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
   * In this test scenario, there are three requests: R0, R1, and R2.
   * At time t0: R0 is made and get served
   * At time t0 + delta: R1 and R2 are made and get blocked.
   * At time t1: R1 gets timeout.
   * At time t2: R0 releases its buffer back to the buffer pool.
   * At time t2 + delta: R2 gets served.
   */
  @Test
  public void testOneExpiredAnotherServed() throws Exception {
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
    }
    consumerThreads[0].start(); // Time t0
    if (!allocated[0].await(100, TimeUnit.MILLISECONDS)) {
      fail("SimpleByteBufferPool takes too long time to allocate a buffer to BufferConsumer[0].");
    }
    consumerThreads[1].start(); // Time t0 + delta
    consumerThreads[2].start(); // Time t0 + delta
    consumerThreads[1].join(); // wait until consumers[1] timeout
    if (!(consumers[1].exception instanceof TimeoutException)) {
      throw consumers[1].exception;
    }
    used[0].countDown(); // Time t2, R0 releases its buffer back to the buffer pool.
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

  /**
   * An entity that has its own thread to request and release {@link ByteBuffer} from a {@link SimpleByteBufferPool}.
   * {@link CountDownLatch} is employed to ensure that a requested buffer has been allocated and the same buffer has
   * been fully deallocated back to the buffer pool.
   */
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
        if (used.await(1000, TimeUnit.MILLISECONDS)) {
          pool.deallocate(buffer);
        } else {
          exception = new IllegalStateException("BufferConsumer takes too long time to release buffer.");
        }
      } catch (Exception e) {
        exception = e;
      }
    }
  }
}
