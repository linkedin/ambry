package com.github.ambry.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;


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
      throws IOException, TimeoutException, InterruptedException {
    long capacity = 2 * 1024;
    int size = 1024;
    long maxBlockTimeMs = 200;
    ByteBufferPool pool = getBufferPool(bufferPoolType, capacity);
    assertEquals("The total allocated pool size should be correct.", capacity, pool.capacity());
    ByteBuffer buffer = pool.allocate(size, maxBlockTimeMs);
    assertEquals("Buffer size should equal requested size.", size, buffer.capacity());
    assertEquals("Available memory should have been decreased", capacity - size, pool.availableMemory());
    pool.deallocate(buffer);
    assertEquals("All memory should be available", capacity, pool.availableMemory());
  }

  /**
   * Scenario: requested size is larger than pool capacity.
   */
  @Test
  public void testRequestExceedPoolCapacity()
      throws TimeoutException, InterruptedException {
    long capacity = 1024;
    long maxBlockTimeMs = 200;
    ByteBufferPool pool = getBufferPool(bufferPoolType, capacity);
    try {
      ByteBuffer buffer = pool.allocate((int) capacity + 1, maxBlockTimeMs);
      fail("Should have thrown!");
    } catch (IOException e) {
      assertEquals("Requested size cannot exceed pool capacity.", e.getMessage());
    }
  }

  /**
   * Scenario: buffer pool exceeds its capacity after reclaiming buffers.
   */
  @Test
  public void testDeallocateExceedPoolCapacity()
      throws TimeoutException, InterruptedException {
    int capacity = 1024;
    ByteBufferPool pool = getBufferPool(bufferPoolType, capacity);
    ByteBuffer singleBuffer = ByteBuffer.allocate(1);
    try {
      pool.deallocate(singleBuffer);
      fail("Should have thrown!");
    } catch (IOException e) {
      assertEquals("Total buffer size cannot exceed pool capacity", e.getMessage());
    }
  }

  /**
   * Scenario: timeout after not enough memory.
   */
  @Test
  public void testNotEnoughMemory()
      throws IOException, InterruptedException {
    int size = 1024;
    long capacity = 1024;
    long maxBlockTimeMs = 20;
    ByteBufferPool pool = getBufferPool(bufferPoolType, capacity);
    try {
      ByteBuffer buffer1 = pool.allocate(size, maxBlockTimeMs);
    } catch (TimeoutException e) {
      fail("Should not happen.");
    }
    try {
      ByteBuffer buffer2 = pool.allocate(size, maxBlockTimeMs);
      fail("should have thrown.");
    } catch (TimeoutException e) {
    }
  }

  /**
   * Scenario: a request is first blocked, and then served before timeout.
   */
  @Test
  public void testFirstBlockedThenSucceed()
      throws InterruptedException {
    ConcurrentLinkedQueue<Integer> expired = new ConcurrentLinkedQueue<Integer>();
    ConcurrentLinkedQueue<Integer> provisioned = new ConcurrentLinkedQueue<Integer>();
    Boolean exceptionFlag = false;
    int size = 1024;
    long capacity = 1024;
    long maxBlockTimeMs = 20000;
    CountDownLatch allocated1 = new CountDownLatch(1);
    CountDownLatch allocated2 = new CountDownLatch(1);
    CountDownLatch used1 = new CountDownLatch(1);
    CountDownLatch used2 = new CountDownLatch(1);
    ByteBufferPool pool = getBufferPool(bufferPoolType, capacity);
    Thread consumer1 = new Thread(
        new BufferConsumer(0, size, maxBlockTimeMs, pool, allocated1, used1, provisioned, expired, exceptionFlag));
    Thread consumer2 = new Thread(
        new BufferConsumer(1, size, maxBlockTimeMs, pool, allocated2, used2, provisioned, expired, exceptionFlag));
    consumer1.start();
    allocated1.await();
    assertEquals(0, pool.availableMemory());
    consumer2.start();
    used1.countDown();
    used2.countDown();
    consumer1.join();
    consumer2.join();
    assertEquals(2, provisioned.size());
    assertEquals(0, expired.size());
    assertEquals(capacity, pool.availableMemory());
    assertFalse(exceptionFlag.booleanValue());
  }

  /**
   * Scenario: n requests are first blocked, and then these requests are served after
   * a large buffer is released back to the pool.
   */
  @Test
  public void testMultipleRequestedServedAfterBlocked()
      throws InterruptedException {
    ConcurrentLinkedQueue<Integer> expired = new ConcurrentLinkedQueue<Integer>();
    ConcurrentLinkedQueue<Integer> provisioned = new ConcurrentLinkedQueue<Integer>();
    Boolean exceptionFlag = false;
    int n = 3;
    int smallSize = 1024;
    int largeSize = n * smallSize;
    long capacity = n * smallSize;
    long maxBlockTimeMs = 200;
    ByteBufferPool pool = getBufferPool(bufferPoolType, capacity);
    CountDownLatch largeAllocated = new CountDownLatch(1);
    CountDownLatch largeUsed = new CountDownLatch(1);
    CountDownLatch smallAllocated = new CountDownLatch(n);
    CountDownLatch smallUsed = new CountDownLatch(n);
    BufferConsumer consumer =
        new BufferConsumer(9, largeSize, maxBlockTimeMs, pool, largeAllocated, largeUsed, provisioned, expired,
            exceptionFlag);
    new Thread(consumer).start();
    largeAllocated.await();
    assertEquals(0, pool.availableMemory());

    Thread[] consumerThreads = new Thread[n];
    for (int i = 0; i < n; i++) {
      consumer = new BufferConsumer(i, smallSize, maxBlockTimeMs, pool, smallAllocated, smallUsed, provisioned, expired,
          exceptionFlag);
      consumerThreads[i] = new Thread(consumer);
      consumerThreads[i].start();
    }

    largeUsed.countDown();
    smallAllocated.await();
    assertEquals(0, pool.availableMemory());
    for (int i = 0; i < n; i++) {
      smallUsed.countDown();
    }
    for (int i = 0; i < n; i++) {
      consumerThreads[i].join();
    }
    assertEquals(capacity, pool.availableMemory());
    assertEquals(0, expired.size());
    assertEquals(1 + n, provisioned.size());
    assertEquals(capacity, pool.availableMemory());
    assertFalse(exceptionFlag.booleanValue());
  }

  /**
   * Scenario: three requests R1, R2, and R3.
   * t0: R1 served, R2 and R3 blocked.
   * t1: R2 timeout.
   * t2: R1 deallocated and R3 served.
   */
  @Test
  public void testOneExpiredAnotherServed()
      throws InterruptedException {
    ConcurrentLinkedQueue<Integer> expired = new ConcurrentLinkedQueue<Integer>();
    ConcurrentLinkedQueue<Integer> provisioned = new ConcurrentLinkedQueue<Integer>();
    Boolean exceptionFlag = false;
    int size = 1024;
    long capacity = 1024;
    int numOfRequests = 3;
    long[] blockTimeMs = {10, 10, 100};
    CountDownLatch[] allocated = new CountDownLatch[numOfRequests];
    CountDownLatch[] used = new CountDownLatch[numOfRequests];
    for (int i = 0; i < numOfRequests; i++) {
      allocated[i] = new CountDownLatch(1);
      used[i] = new CountDownLatch(1);
    }
    ByteBufferPool pool = getBufferPool(bufferPoolType, capacity);
    BufferConsumer consumer = null;
    Thread[] consumerThreads = new Thread[numOfRequests];
    for (int i = 0; i < numOfRequests; i++) {
      consumer =
          new BufferConsumer(i, size, blockTimeMs[i], pool, allocated[i], used[i], provisioned, expired, exceptionFlag);
      consumerThreads[i] = new Thread(consumer);
      consumerThreads[i].start();
    }
    Thread.sleep(50);
    assertEquals(1, expired.size());
    used[0].countDown();
    used[2].countDown();
    for (int i = 0; i < numOfRequests; i++) {
      consumerThreads[i].join();
    }
    assertEquals(2, provisioned.size());
    assertEquals(1, expired.size());
    assertEquals(capacity, pool.availableMemory());
    assertFalse(exceptionFlag.booleanValue());
  }

  private class BufferConsumer implements Runnable {
    private int id = -1;
    private int size = 0;
    private long maxBlockTimeMs = -1;
    private ByteBufferPool pool = null;
    private ByteBuffer buffer = null;
    private CountDownLatch allocated = null;
    private CountDownLatch used = null;
    private ConcurrentLinkedQueue<Integer> expired;
    private ConcurrentLinkedQueue<Integer> provisioned;
    private Boolean exceptionFlag;

    private BufferConsumer(int id, int size, long maxBlockTimeMs, ByteBufferPool pool, CountDownLatch allocated,
        CountDownLatch used, ConcurrentLinkedQueue<Integer> provisioned, ConcurrentLinkedQueue<Integer> expired,
        Boolean exceptionFlag) {
      this.id = id;
      this.size = size;
      this.maxBlockTimeMs = maxBlockTimeMs;
      this.pool = pool;
      this.allocated = allocated;
      this.used = used;
      this.expired = expired;
      this.provisioned = provisioned;
      this.exceptionFlag = exceptionFlag;
    }

    @Override
    public void run() {
      try {
        buffer = pool.allocate(size, maxBlockTimeMs);
        allocated.countDown();
        used.await();
      } catch (IOException e) {
        exceptionFlag = true;
      } catch (InterruptedException e) {
        exceptionFlag = true;
      } catch (TimeoutException e) {
        expired.add(id);
        return;
      }
      try {
        pool.deallocate(buffer);
      } catch (IOException e) {
        exceptionFlag = true;
      }
      provisioned.add(id);
    }
  }

  ByteBufferPool getBufferPool(String bufferPoolType, long size) {
    if (bufferPoolType.equals("Simple")) {
      return new SimpleByteBufferPool(size);
    } else if (bufferPoolType.equals("Fifo")) {
      return new FifoByteBufferPool(size);
    } else {
      fail("Invalid pool type.");
      return null;
    }
  }
}
