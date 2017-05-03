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
package com.github.ambry.commons;

import com.github.ambry.router.AsyncWritableChannel;
import com.github.ambry.router.Callback;
import com.github.ambry.router.FutureResult;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;


/**
 * Tests functionality of {@link ByteBufferReadableStreamChannel}.
 */
@RunWith(Parameterized.class)
public class ByteBufferReadableStreamChannelTest {
  public static final int BUFFER_SIZE = 256;

  private ByteBuffer contentWrapper;
  private ByteBufferReadableStreamChannel byteBufferReadableStreamChannel;

  public ByteBufferReadableStreamChannelTest(List<ByteBuffer> bufferList) {
    if (bufferList.size() == 1) {
      byteBufferReadableStreamChannel = new ByteBufferReadableStreamChannel(bufferList.get(0));
    } else {
      byteBufferReadableStreamChannel = new ByteBufferReadableStreamChannel(bufferList);
    }
    contentWrapper = ByteBuffer.allocate((int) byteBufferReadableStreamChannel.getSize());
    for (ByteBuffer buffer: bufferList) {
      contentWrapper.put(buffer.duplicate());
    }
    contentWrapper.rewind();
  }

  @Parameterized.Parameters
  public static List<Object[]> data() {
    // @formatter:off
    return Arrays.asList(new Object[][] {
      {Arrays.asList(ByteBuffer.wrap(TestUtils.getRandomBytes(BUFFER_SIZE)))},
      {Arrays.asList(ByteBuffer.wrap(TestUtils.getRandomBytes(BUFFER_SIZE)), ByteBuffer.wrap(TestUtils.getRandomBytes(BUFFER_SIZE)))}
    });
    // @formatter:on
  }

  /**
   * Tests the common case read operations i.e
   * Calls the different read operations of {@link ByteBufferReadableStreamChannel} and checks that the data read
   * matches the data used to create the {@link ByteBufferReadableStreamChannel}.
   * @throws Exception
   */
  @Test
  public void commonCaseTest() throws Exception {
    assertTrue("ByteBufferReadableStreamChannel is not open", byteBufferReadableStreamChannel.isOpen());
    assertEquals("Size returned by ByteBufferReadableStreamChannel did not match source array size",
        contentWrapper.capacity(), byteBufferReadableStreamChannel.getSize());
    ByteBufferAsyncWritableChannel writeChannel = new ByteBufferAsyncWritableChannel();
    ReadIntoCallback callback = new ReadIntoCallback();
    Future<Long> future = byteBufferReadableStreamChannel.readInto(writeChannel, callback);
    while (contentWrapper.hasRemaining()) {
      ByteBuffer recvdContent = writeChannel.getNextChunk();
      assertNotNull("Written content lesser than original content", recvdContent);
      while (recvdContent.hasRemaining()) {
        assertTrue("Written content is more than original content", contentWrapper.hasRemaining());
        assertEquals("Unexpected byte", contentWrapper.get(), recvdContent.get());
      }
      writeChannel.resolveOldestChunk(null);
    }
    assertNull("There should have been no more data in the channel", writeChannel.getNextChunk(0));
    writeChannel.close();
    callback.awaitCallback();
    if (callback.exception != null) {
      throw callback.exception;
    }
    long futureBytesRead = future.get();
    assertEquals("Total bytes written does not match (callback)", contentWrapper.capacity(), callback.bytesRead);
    assertEquals("Total bytes written does not match (future)", contentWrapper.capacity(), futureBytesRead);
  }

  /**
   * Tests that the right exceptions are thrown when {@link AsyncWritableChannel} write fails.
   * @throws Exception
   */
  @Test
  public void readIntoAWCFailureTest() throws Exception {
    String errMsg = "@@ExpectedExceptionMessage@@";
    // Bad AWC.
    ReadIntoCallback callback = new ReadIntoCallback();
    try {
      byteBufferReadableStreamChannel.readInto(new BadAsyncWritableChannel(new IOException(errMsg)), callback).get();
      fail("Should have failed because BadAsyncWritableChannel would have thrown exception");
    } catch (ExecutionException e) {
      Exception exception = (Exception) Utils.getRootCause(e);
      assertEquals("Exception message does not match expected (future)", errMsg, exception.getMessage());
      callback.awaitCallback();
      assertEquals("Exception message does not match expected (callback)", errMsg, callback.exception.getMessage());
    }
  }

  /**
   * Tests that the right exceptions are thrown when reading more than once from {@link ByteBufferReadableStreamChannel}
   * @throws Exception
   */
  @Test
  public void readMoreThanOnceTest() throws Exception {
    // Reading more than once.
    ByteBufferAsyncWritableChannel writeChannel = new ByteBufferAsyncWritableChannel();
    byteBufferReadableStreamChannel.readInto(writeChannel, null);
    try {
      byteBufferReadableStreamChannel.readInto(writeChannel, null);
      fail("Should have failed because readInto cannot be called more than once");
    } catch (IllegalStateException e) {
      // expected. Nothing to do.
    }
  }

  /**
   * Tests that the right exceptions are trying to read after a {@link ByteBufferReadableStreamChannel} is closed.
   * @throws Exception
   */
  @Test
  public void readAfterCloseTest() throws Exception {
    // Read after close.
    byteBufferReadableStreamChannel.close();
    ByteBufferAsyncWritableChannel writeChannel = new ByteBufferAsyncWritableChannel();
    ReadIntoCallback callback = new ReadIntoCallback();
    try {
      byteBufferReadableStreamChannel.readInto(writeChannel, callback).get();
      fail("ByteBufferReadableStreamChannel has been closed, so read should have thrown ClosedChannelException");
    } catch (ExecutionException e) {
      Exception exception = (Exception) Utils.getRootCause(e);
      assertTrue("Exception is not ClosedChannelException", exception instanceof ClosedChannelException);
      callback.awaitCallback();
      assertEquals("Exceptions of callback and future differ", exception.getMessage(), callback.exception.getMessage());
    }
  }

  /**
   * Tests that no exceptions are thrown on repeating idempotent operations. Does <b><i>not</i></b> currently test that
   * state changes are idempotent.
   * @throws IOException
   */
  @Test
  public void idempotentOperationsTest() throws IOException {
    assertTrue("ByteBufferReadableStreamChannel is not open", byteBufferReadableStreamChannel.isOpen());
    byteBufferReadableStreamChannel.close();
    assertFalse("ByteBufferReadableStreamChannel is not closed", byteBufferReadableStreamChannel.isOpen());
    // should not throw exception.
    byteBufferReadableStreamChannel.close();
    assertFalse("ByteBufferReadableStreamChannel is not closed", byteBufferReadableStreamChannel.isOpen());
  }
}

/**
 * Callback for read operations on {@link ByteBufferReadableStreamChannel}.
 */
class ReadIntoCallback implements Callback<Long> {
  public volatile long bytesRead;
  public volatile Exception exception;
  private final AtomicBoolean callbackInvoked = new AtomicBoolean(false);
  private final CountDownLatch latch = new CountDownLatch(1);

  @Override
  public void onCompletion(Long result, Exception exception) {
    if (callbackInvoked.compareAndSet(false, true)) {
      bytesRead = result;
      this.exception = exception;
      latch.countDown();
    } else {
      this.exception = new IllegalStateException("Callback invoked more than once");
    }
  }

  /**
   * Waits for the callback to arrive for a limited amount of time.
   * @throws InterruptedException
   * @throws TimeoutException
   */
  void awaitCallback() throws InterruptedException, TimeoutException {
    if (!latch.await(1, TimeUnit.SECONDS)) {
      throw new TimeoutException("Waiting too long for callback to arrive");
    }
  }
}

/**
 * A {@link AsyncWritableChannel} that throws a custom exception (provided at construction time) on a call to
 * {@link #write(ByteBuffer, Callback)}.
 */
class BadAsyncWritableChannel implements AsyncWritableChannel {
  private final Exception exceptionToThrow;
  private final AtomicBoolean isOpen = new AtomicBoolean(true);

  /**
   * Creates an instance of BadAsyncWritableChannel that throws {@code exceptionToThrow} on write.
   * @param exceptionToThrow the {@link Exception} to throw on write.
   */
  public BadAsyncWritableChannel(Exception exceptionToThrow) {
    this.exceptionToThrow = exceptionToThrow;
  }

  @Override
  public Future<Long> write(ByteBuffer src, Callback<Long> callback) {
    if (exceptionToThrow instanceof RuntimeException) {
      throw (RuntimeException) exceptionToThrow;
    } else {
      return markFutureInvokeCallback(callback, 0, exceptionToThrow);
    }
  }

  @Override
  public boolean isOpen() {
    return isOpen.get();
  }

  @Override
  public void close() throws IOException {
    isOpen.set(false);
  }

  /**
   * Creates and marks a future as done and invoked the callback with paramaters {@code totalBytesWritten} and
   * {@code Exception}.
   * @param callback the {@link Callback} to invoke.
   * @param totalBytesWritten the number of bytes successfully written.
   * @param exception the {@link Exception} that occurred if any.
   * @return the {@link Future} that will contain the result of the operation.
   */
  private Future<Long> markFutureInvokeCallback(Callback<Long> callback, long totalBytesWritten, Exception exception) {
    FutureResult<Long> futureResult = new FutureResult<Long>();
    futureResult.done(totalBytesWritten, exception);
    if (callback != null) {
      callback.onCompletion(totalBytesWritten, exception);
    }
    return futureResult;
  }
}
