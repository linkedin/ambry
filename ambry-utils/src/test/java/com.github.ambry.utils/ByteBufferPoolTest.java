package com.github.ambry.utils;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;


/**
 * This class tests two bounded {@link ByteBufferPool} implementations
 */
@RunWith(Parameterized.class)
public class ByteBufferPoolTest {
  private String bufferPoolType;

  @Parameterized.Parameters
  public static java.util.Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{{"Simple"}, {"Fifo"}});
  }

  public ByteBufferPoolTest(String bufferPoolType) {
    this.bufferPoolType = bufferPoolType;
  }

  /**
   * Scenario: simple non-blocking allocation and deallocation.
   */
  @Test
  public void testSingleRequestAllocateDeallocate()
      throws TimeoutException, InterruptedException {
    final long capacity = 2 * 1024;
    final int size = 1024;
    long maxBlockTimeInMs = 200;
    ByteBufferPool pool = getBufferPool(capacity);
    assertEquals(capacity, getCapacityMemory(pool));
    ByteBuffer buffer = pool.allocate(size, maxBlockTimeInMs);
    assertEquals(size, buffer.capacity());
    assertEquals(capacity - size, getAvailableMemory(pool));
    pool.deallocate(buffer);
    assertEquals(capacity, getCapacityMemory(pool));
  }

  /**
   * Scenario: requested size is larger than pool capacity.
   */
  @Test
  public void testRequestExceedPoolCapacity()
      throws TimeoutException, InterruptedException {
    final long capacity = 1024;
    final long maxBlockTimeInMs = 200;
    ByteBufferPool pool = getBufferPool(capacity);
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
    ByteBufferPool pool = getBufferPool(capacity);
    ByteBuffer singleBuffer = ByteBuffer.allocate(1);
    pool.deallocate(singleBuffer);
    assertEquals(capacity, getAvailableMemory(pool));
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
    ByteBufferPool pool = getBufferPool(capacity);
    ByteBuffer buffer1 = pool.allocate(size, maxBlockTimeInMs);
    try {
      ByteBuffer buffer2 = pool.allocate(size, maxBlockTimeInMs);
      fail("should have thrown.");
    } catch (TimeoutException e) {
    }
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
    ByteBufferPool pool = getBufferPool(capacity);
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
    assertEquals(0, getAvailableMemory(pool));
    for (int i = 0; i < 2; i++) {
      used[i].countDown();
      consumerThreads[i].join();
      if (consumers[i].exception != null) {
        throw consumers[i].exception;
      }
    }
    assertEquals(capacity, getAvailableMemory(pool));
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
    ByteBufferPool pool = getBufferPool(capacity);
    CountDownLatch largeAllocated = new CountDownLatch(1);
    CountDownLatch largeUsed = new CountDownLatch(1);
    CountDownLatch smallAllocated = new CountDownLatch(n);
    CountDownLatch smallUsed = new CountDownLatch(n);
    BufferConsumer largeConsumer = new BufferConsumer(largeSize, maxBlockTimeInMs, pool, largeAllocated, largeUsed);
    new Thread(largeConsumer).start();
    largeAllocated.await();
    assertEquals(0, getAvailableMemory(pool));
    BufferConsumer[] smallConsumers = new BufferConsumer[n];
    Thread[] smallConsumerThreads = new Thread[n];
    for (int i = 0; i < n; i++) {
      smallConsumers[i] = new BufferConsumer(smallSize, maxBlockTimeInMs, pool, smallAllocated, smallUsed);
      smallConsumerThreads[i] = new Thread(smallConsumers[i]);
      smallConsumerThreads[i].start();
    }
    largeUsed.countDown();
    smallAllocated.await();
    assertEquals(0, getAvailableMemory(pool));
    for (int i = 0; i < n; i++) {
      smallUsed.countDown();
    }
    for (int i = 0; i < n; i++) {
      smallConsumerThreads[i].join();
    }
    assertEquals(capacity, getAvailableMemory(pool));
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
    ByteBufferPool pool = getBufferPool(capacity);
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
    }
    if (consumers[2].exception != null) {
      throw consumers[2].exception;
    }
    assertEquals(capacity, getAvailableMemory(pool));
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

  ByteBufferPool getBufferPool(long size) {
    if (bufferPoolType.equals("Simple")) {
      return new SimpleByteBufferPool(size);
    } else if (bufferPoolType.equals("Fifo")) {
      return new FifoByteBufferPool(size);
    } else {
      fail("Invalid pool type.");
      throw new IllegalArgumentException();
    }
  }

  long getAvailableMemory(ByteBufferPool pool) {
    if (pool instanceof FifoByteBufferPool) {
      return ((FifoByteBufferPool) pool).availableMemory();
    } else if (pool instanceof SimpleByteBufferPool) {
      return ((SimpleByteBufferPool) pool).availableMemory();
    } else {
      throw new IllegalArgumentException();
    }
  }

  long getCapacityMemory(ByteBufferPool pool) {
    if (pool instanceof FifoByteBufferPool) {
      return ((FifoByteBufferPool) pool).capacity();
    } else if (pool instanceof SimpleByteBufferPool) {
      return ((SimpleByteBufferPool) pool).capacity();
    } else {
      throw new IllegalArgumentException();
    }
  }
}
