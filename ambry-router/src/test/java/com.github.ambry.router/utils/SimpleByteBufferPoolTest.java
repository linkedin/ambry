package com.github.ambry.router.utils;

/**
 * Unit test for {@link com.github.ambry.router.utils.SimpleByteBufferPool}
 */

import com.github.ambry.router.RouterErrorCode;
import com.github.ambry.router.RouterException;
import com.github.ambry.utils.SystemTime;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;


public class SimpleByteBufferPoolTest {
  private final long maxBlockTimeMs = 2000;

  /**
   * Test the simple non-blocking allocation.
   */
  @Test
  public void testAllocateDeallocate()
      throws RouterException, InterruptedException {
    long capacity = 2 * 1024;
    int size = 1024;
    ByteBufferPool pool = new SimpleByteBufferPool(capacity);
    assertEquals("The total allocated pool size should be correct.", capacity, pool.capacity());
    ByteBuffer buffer = pool.allocate(size, maxBlockTimeMs);
    assertEquals("Buffer size should equal requested size.", size, buffer.capacity());
    assertEquals("Available memory should have been decreased", capacity - size, pool.availableMemory());
    pool.deallocate(buffer);
    assertEquals("All memory should be available", capacity, pool.availableMemory());
  }

  /**
   * Test that we cannot allocate bytebuffers that are larger than the size of the pool.
   */
  @Test
  public void testRequestExceedPoolCapacity()
      throws InterruptedException {
    int capacity = 1024;
    ByteBufferPool pool = new SimpleByteBufferPool(capacity);
    try {
      ByteBuffer buffer = pool.allocate(capacity + 1, maxBlockTimeMs);
      fail("Should have thrown!");
    } catch (RouterException e) {
      assertEquals(RouterErrorCode.ExceedPoolCapacity, e.getErrorCode());
    }
  }

  /**
   * Test when there is not enough memory to allocate until a pre-configured
   * wait time has elasped.
   */
  @Test
  public void testNotEnoughMemory()
      throws InterruptedException {
    int size = 1024;
    long capacity = 2 * 1024;
    ByteBufferPool pool = new SimpleByteBufferPool(capacity);
    long requestTime = 0;
    long failTime = -1;
    try {
      ByteBuffer buffer1 = pool.allocate(size, maxBlockTimeMs);
      requestTime = SystemTime.getInstance().milliseconds();
      ByteBuffer buffer2 = pool.allocate(size + 1, maxBlockTimeMs);
      fail("should have thrown.");
    } catch (RouterException e) {
      failTime = SystemTime.getInstance().milliseconds();
      assertEquals(RouterErrorCode.BufferPoolNotEnoughMemory, e.getErrorCode());
      assertTrue(failTime - requestTime >= maxBlockTimeMs);
    }
  }

  /**
   * Test when a request is first blocked due to not enough memory, and then
   * served before timeout because the pool is able to allocated the requested size.
   */
  @Test
  public void testFirstBlockedThenSucceed()
      throws InterruptedException {
    int size = 1024;
    long capacity = 1024;
    CountDownLatch allocationComplete1 = new CountDownLatch(1);
    CountDownLatch allocationComplete2 = new CountDownLatch(1);
    ByteBufferPool pool = new SimpleByteBufferPool(capacity);
    BufferConsumer bufferConsumer1 = new BufferConsumer(size, 500, pool, allocationComplete1);
    BufferConsumer bufferConsumer2 = new BufferConsumer(size, 500, pool, allocationComplete2);
    Thread consumerThread1 = new Thread(bufferConsumer1);
    Thread consumerThread2 = new Thread(bufferConsumer2);
    consumerThread1.start();
    allocationComplete1.await();
    assertEquals(0, pool.availableMemory());
    consumerThread2.start();
    allocationComplete2.await();
    assertEquals(bufferConsumer2.isTimeout(), false);
    assertNotNull(bufferConsumer2.buffer);
    assertEquals(0, pool.availableMemory());
  }

  private class BufferConsumer implements Runnable {
    private int size = 0;
    private long useTimeMs = 0;
    private ByteBufferPool pool = null;
    private boolean timeout = false;
    private ByteBuffer buffer = null;
    private CountDownLatch complete = null;

    private BufferConsumer(int size, long useTimeMs, ByteBufferPool pool, CountDownLatch complete) {
      this.size = size;
      this.useTimeMs = useTimeMs;
      this.pool = pool;
      this.complete = complete;
    }

    private boolean isTimeout() {
      return timeout;
    }

    @Override
    public void run() {
      try {
        buffer = pool.allocate(size, maxBlockTimeMs);
        complete.countDown();
        Thread.sleep(useTimeMs);
      } catch (InterruptedException e) {
        fail("Interruption should not happen.");
      } catch (RouterException e) {
        if (e.getErrorCode() == RouterErrorCode.BlobExpired) {
          this.timeout = true;
        }
      }
      pool.deallocate(buffer);
    }
  }

  /**
   * Test multiple requests can be served once a large buffer is released back
   * to the pool.
   */
  @Test
  public void testMultipleRequestedServed()
      throws InterruptedException {
    int bulkSize = 3 * 1024;
    int smallSize = 1024;
    long capacity = 3 * 1024;
    int numOfSmallRequests = 4;
    long bulkUseTime = 200;
    long smallUseTime = 500;
    ByteBufferPool pool = new SimpleByteBufferPool(capacity);
    CountDownLatch bulkAllocated = new CountDownLatch(1);
    BufferConsumer bufferConsumer = new BufferConsumer(bulkSize, bulkUseTime, pool, bulkAllocated);
    new Thread(bufferConsumer).start();
    bulkAllocated.await();
    assertEquals(0, pool.availableMemory());
    for (int i = 0; i < numOfSmallRequests; i++) {
      bufferConsumer = new BufferConsumer(smallSize, smallUseTime, pool, new CountDownLatch(1));
      new Thread(bufferConsumer).start();
    }
    Thread.sleep(10); //wait until all the 4 requests are blocked.
    assertEquals(numOfSmallRequests, pool.queued());
    Thread.sleep(smallUseTime);
    assertEquals(0, pool.availableMemory());
    assertEquals(1, pool.queued());
  }
}
