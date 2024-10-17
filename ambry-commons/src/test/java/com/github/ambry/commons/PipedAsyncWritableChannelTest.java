/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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
 *
 */

package com.github.ambry.commons;

import com.github.ambry.router.AsyncWritableChannel;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.utils.NettyByteBufLeakHelper;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


public class PipedAsyncWritableChannelTest {

  private final NettyByteBufLeakHelper nettyByteBufLeakHelper = new NettyByteBufLeakHelper();

  @Before
  public void before() {
    nettyByteBufLeakHelper.beforeTest();
  }

  @After
  public void after() {
    nettyByteBufLeakHelper.afterTest();
  }

  /**
   * Verify we are able to read contents from primary readable channel with secondary absent
   * @throws Exception
   */
  @Test
  public void primaryReadTest() throws Exception {
    ByteBuffer content = ByteBuffer.wrap(fillRandomBytes(new byte[1024]));
    ByteBufferReadableStreamChannel sourceReadableStreamChannel = new ByteBufferReadableStreamChannel(content);
    PipedAsyncWritableChannel pipedAsyncWritableChannel =
        new PipedAsyncWritableChannel(sourceReadableStreamChannel, false, 100);
    ReadableStreamChannel pipedPrimaryReadableStreamChannel =
        pipedAsyncWritableChannel.getPrimaryReadableStreamChannel();
    assertNotNull("Primary readable stream channel must not be null", pipedPrimaryReadableStreamChannel);
    assertNull("Secondary readable stream channel must be null",
        pipedAsyncWritableChannel.getSecondaryReadableStreamChannel());
    verifyChannelRead(pipedPrimaryReadableStreamChannel, new ByteBufferAsyncWritableChannel(), content, null);
  }

  /**
   * Verify we are able to read contents from primary and secondary read channels
   */
  @Test
  public void primaryAndSecondaryReadTest() {
    ByteBuffer content = ByteBuffer.wrap(fillRandomBytes(new byte[1024]));
    ByteBufferReadableStreamChannel sourceReadableStreamChannel = new ByteBufferReadableStreamChannel(content);
    PipedAsyncWritableChannel pipedAsyncWritableChannel =
        new PipedAsyncWritableChannel(sourceReadableStreamChannel, true, 100);
    ReadableStreamChannel primaryReadableStreamChannel = pipedAsyncWritableChannel.getPrimaryReadableStreamChannel();
    ReadableStreamChannel secondaryReadableStreamChannel =
        pipedAsyncWritableChannel.getSecondaryReadableStreamChannel();
    assertNotNull("Primary readable stream channel must not be null", primaryReadableStreamChannel);
    assertNotNull("Secondary readable stream channel must not be null", secondaryReadableStreamChannel);

    // Verify we are able to read contents from both primary and secondary readable channels
    ExecutorService executorService = Executors.newFixedThreadPool(2);
    executorService.execute(() -> {
      try {
        verifyChannelRead(primaryReadableStreamChannel, new ByteBufferAsyncWritableChannel(), content, null);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    executorService.execute(() -> {
      try {
        verifyChannelRead(secondaryReadableStreamChannel, new ByteBufferAsyncWritableChannel(), content, null);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Test
  public void primaryReadByteBufTest() throws Exception {
    ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.heapBuffer(1024);
    byteBuf.writeBytes(fillRandomBytes(new byte[1024]));
    byteBuf.retain(); // retain before it goes to readable stream channel
    try {
      PipedAsyncWritableChannel pipedAsyncWritableChannel =
          new PipedAsyncWritableChannel(new ByteBufReadableStreamChannel(byteBuf), false, 100);
      ReadableStreamChannel primaryReadableStreamChannel = pipedAsyncWritableChannel.getPrimaryReadableStreamChannel();
      ByteBufferAsyncWritableChannel writableChannel = new ByteBufferAsyncWritableChannel();
      primaryReadableStreamChannel.readInto(writableChannel, null);
      ByteBuf obtained = writableChannel.getNextByteBuf();
      // Make sure this is the same as original byte array
      obtained.retain();
      writableChannel.resolveOldestChunk(null);
      for (int i = 0; i < 1024; i++) {
        Assert.assertEquals(byteBuf.getByte(i), obtained.getByte(i));
      }
      obtained.release();
    } finally {
      byteBuf.release();
      assertEquals("Reference count of the original byte buf must be back to 0", 0, byteBuf.refCnt());
    }
  }

  @Test
  public void primaryAndSecondaryReadByteBufTest() throws Exception {
    ByteBuf byteBuf = PooledByteBufAllocator.DEFAULT.heapBuffer(1024);
    byteBuf.writeBytes(fillRandomBytes(new byte[1024]));
    byteBuf.retain(); // retain before it goes to readable stream channel
    try {
      PipedAsyncWritableChannel pipedAsyncWritableChannel =
          new PipedAsyncWritableChannel(new ByteBufReadableStreamChannel(byteBuf), true, 100);

      // Verify we are able to read contents from both primary and secondary readable channels
      ExecutorService executorService = Executors.newFixedThreadPool(2);
      Future<?> primaryReadFuture = executorService.submit(() -> {
        try {
          ReadableStreamChannel primaryReadableStreamChannel =
              pipedAsyncWritableChannel.getPrimaryReadableStreamChannel();
          ByteBufferAsyncWritableChannel writableChannel = new ByteBufferAsyncWritableChannel();
          primaryReadableStreamChannel.readInto(writableChannel, null);
          ByteBuf obtained = writableChannel.getNextByteBuf();
          // Make sure this is the same as original byte array
          obtained.retain();
          writableChannel.resolveOldestChunk(null);
          for (int i = 0; i < 1024; i++) {
            assertEquals(byteBuf.getByte(i), obtained.getByte(i));
          }
          obtained.release();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });

      Future<?> secondaryReadFuture = executorService.submit(() -> {
        try {
          ReadableStreamChannel primaryReadableStreamChannel =
              pipedAsyncWritableChannel.getSecondaryReadableStreamChannel();
          ByteBufferAsyncWritableChannel writableChannel = new ByteBufferAsyncWritableChannel();
          primaryReadableStreamChannel.readInto(writableChannel, null);
          ByteBuf obtained = writableChannel.getNextByteBuf();
          // Make sure this is the same as original byte array
          obtained.retain();
          writableChannel.resolveOldestChunk(null);
          for (int i = 0; i < 1024; i++) {
            assertEquals(byteBuf.getByte(i), obtained.getByte(i));
          }
          obtained.release();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });

      primaryReadFuture.get(500, TimeUnit.MILLISECONDS);
      secondaryReadFuture.get(500, TimeUnit.MILLISECONDS);
    } finally {
      byteBuf.release();
      assertEquals("Reference count of the original byte buf must be back to 0", 0, byteBuf.refCnt());
    }
  }

  /**
   * Verify that secondary is closed with ClosedChannelException if primary fails
   */
  @Test
  public void primaryFailureTest() {
    ByteBuffer content = ByteBuffer.wrap(fillRandomBytes(new byte[1024]));
    ByteBufferReadableStreamChannel sourceReadableStreamChannel = new ByteBufferReadableStreamChannel(content);
    PipedAsyncWritableChannel pipedAsyncWritableChannel =
        new PipedAsyncWritableChannel(sourceReadableStreamChannel, true, 100);
    ReadableStreamChannel primaryReadableStreamChannel = pipedAsyncWritableChannel.getPrimaryReadableStreamChannel();
    ReadableStreamChannel secondaryReadableStreamChannel =
        pipedAsyncWritableChannel.getSecondaryReadableStreamChannel();
    assertNotNull("Primary readable stream channel must not be null", primaryReadableStreamChannel);
    assertNotNull("Secondary readable stream channel must not be null", secondaryReadableStreamChannel);

    ExecutorService executorService = Executors.newFixedThreadPool(2);
    String errMsg = "@@ExpectedExceptionMessage@@";
    executorService.execute(() -> {
      try {
        verifyChannelRead(primaryReadableStreamChannel, new BadAsyncWritableChannel(new IOException(errMsg)), content,
            new IOException(errMsg));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    // Verify secondary fails with closed channel exception if primary fails
    executorService.execute(() -> {
      try {
        verifyChannelRead(secondaryReadableStreamChannel, new ByteBufferAsyncWritableChannel(), content,
            new ClosedChannelException());
      } catch (Exception e) {
        assertEquals("Exception message does not match expected (future)", errMsg, e.getMessage());
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Verify that primary read is successful even if secondary fails
   */
  @Test
  public void secondaryFailureTest() {
    ByteBuffer content = ByteBuffer.wrap(fillRandomBytes(new byte[1024]));
    ByteBufferReadableStreamChannel sourceReadableStreamChannel = new ByteBufferReadableStreamChannel(content);
    PipedAsyncWritableChannel pipedAsyncWritableChannel =
        new PipedAsyncWritableChannel(sourceReadableStreamChannel, true, 100);
    ReadableStreamChannel primaryReadableStreamChannel = pipedAsyncWritableChannel.getPrimaryReadableStreamChannel();
    ReadableStreamChannel secondaryReadableStreamChannel =
        pipedAsyncWritableChannel.getSecondaryReadableStreamChannel();
    assertNotNull("Primary readable stream channel must not be null", primaryReadableStreamChannel);
    assertNotNull("Secondary readable stream channel must not be null", secondaryReadableStreamChannel);

    // Verify primary succeeds even if secondary fails
    ExecutorService executorService = Executors.newFixedThreadPool(2);
    executorService.execute(() -> {
      try {
        verifyChannelRead(primaryReadableStreamChannel, new ByteBufferAsyncWritableChannel(), content, null);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
    executorService.execute(() -> {
      try {
        String errMsg = "@@ExpectedExceptionMessage@@";
        verifyChannelRead(secondaryReadableStreamChannel, new BadAsyncWritableChannel(new IOException(errMsg)), content,
            new IOException(errMsg));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Tests that the right exceptions are thrown when reading from {@link ReadableStreamChannel} fails.
   * @throws Exception
   */
  @Test
  public void readIntoFailureTest() throws Exception {
    String errMsg = "@@ExpectedExceptionMessage@@";
    byte[] in = fillRandomBytes(new byte[1]);
    ByteBufferReadableStreamChannel readableStreamChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(in));
    PipedAsyncWritableChannel pipedAsyncWritableChannel =
        new PipedAsyncWritableChannel(readableStreamChannel, false, 100);
    ReadableStreamChannel primaryReadableStreamChannel = pipedAsyncWritableChannel.getPrimaryReadableStreamChannel();

    // 1. Bad Async writable channel.
    com.github.ambry.commons.ReadIntoCallback callback = new com.github.ambry.commons.ReadIntoCallback();
    try {
      primaryReadableStreamChannel.readInto(new BadAsyncWritableChannel(new IOException(errMsg)), callback).get();
      fail("Should have failed because BadAsyncWritableChannel would have thrown exception");
    } catch (ExecutionException e) {
      Exception exception = (Exception) Utils.getRootCause(e);
      assertEquals("Exception message does not match expected (future)", errMsg, exception.getMessage());
      callback.awaitCallback();
      assertEquals("Exception message does not match expected (callback)", errMsg, callback.exception.getMessage());
    }

    // 2. Reading more than once.
    primaryReadableStreamChannel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(in));
    ByteBufferAsyncWritableChannel writeChannel = new ByteBufferAsyncWritableChannel();
    primaryReadableStreamChannel.readInto(writeChannel, null);
    try {
      primaryReadableStreamChannel.readInto(writeChannel, null);
      fail("Should have failed because readInto cannot be called more than once");
    } catch (IllegalStateException e) {
      // expected. Nothing to do.
    }

    // 3. Read after close.
    primaryReadableStreamChannel.close();
    writeChannel = new ByteBufferAsyncWritableChannel();
    callback = new com.github.ambry.commons.ReadIntoCallback();
    try {
      primaryReadableStreamChannel.readInto(writeChannel, callback).get();
      fail("ByteBufferReadableStreamChannel has been closed, so read should have thrown ClosedChannelException");
    } catch (ExecutionException e) {
      Exception exception = (Exception) Utils.getRootCause(e);
      assertTrue("Exception is not ClosedChannelException", exception instanceof ClosedChannelException);
      callback.awaitCallback();
      assertEquals("Exceptions of callback and future differ", exception.getMessage(), callback.exception.getMessage());
    }
  }

  /**
   * Verify contents in {@link ReadableStreamChannel} are read into {@link AsyncWritableChannel} correctly
   * @param readableStreamChannel Channel to read from
   * @param writeChannel Channel to write into
   * @param content content
   * @param exceptionExpected expected {@link Exception} in failure cases
   */
  public void verifyChannelRead(ReadableStreamChannel readableStreamChannel, AsyncWritableChannel writeChannel,
      ByteBuffer content, Exception exceptionExpected) throws Exception {
    com.github.ambry.commons.ReadIntoCallback callback = new com.github.ambry.commons.ReadIntoCallback();
    Future<Long> future = readableStreamChannel.readInto(writeChannel, callback);

    if (exceptionExpected != null) {
      try {
        future.get();
        fail("Should have failed with exception");
      } catch (ExecutionException e) {
        Exception exception = (Exception) Utils.getRootCause(e);
        callback.awaitCallback();
        if (exceptionExpected instanceof ClosedChannelException) {
          assertTrue("Exception message does not match expected (future)", exception instanceof ClosedChannelException);
          assertTrue("Exception message does not match expected (callback)",
              callback.exception instanceof ClosedChannelException);
        } else {
          assertEquals("Exception message does not match expected (future)", exceptionExpected.getMessage(),
              exception.getMessage());
          assertEquals("Exception message does not match expected (callback)", exceptionExpected.getMessage(),
              callback.exception.getMessage());
        }
      }
      return;
    }

    ByteBuffer contentWrapper = ByteBuffer.wrap(content.array());
    while (contentWrapper.hasRemaining()) {
      ByteBuffer recvdContent = ((ByteBufferAsyncWritableChannel) writeChannel).getNextChunk();
      assertNotNull("Written content lesser than original content", recvdContent);
      while (recvdContent.hasRemaining()) {
        assertTrue("Written content is more than original content", contentWrapper.hasRemaining());
        assertEquals("Unexpected byte", contentWrapper.get(), recvdContent.get());
      }
      ((ByteBufferAsyncWritableChannel) writeChannel).resolveOldestChunk(null);
    }
    assertNull("There should have been no more data in the channel",
        ((ByteBufferAsyncWritableChannel) writeChannel).getNextChunk(0));
    writeChannel.close();
    callback.awaitCallback();
    long futureBytesRead = future.get();
    assertEquals("Total bytes written does not match (callback)", content.limit(), callback.bytesRead);
    assertEquals("Total bytes written does not match (future)", content.limit(), futureBytesRead);
  }

  /**
   * Fills random bytes into {@code in}.
   * @param in the byte array that needs to be filled with random bytes.
   * @return {@code in} filled with random bytes.
   */
  private byte[] fillRandomBytes(byte[] in) {
    new Random().nextBytes(in);
    return in;
  }
}