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
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.NettyByteBufLeakHelper;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests functionality of {@link InputStreamReadableStreamChannel}.
 */
public class InputStreamReadableStreamChannelTest {
  private static final ExecutorService EXECUTOR_SERVICE = Executors.newSingleThreadExecutor();
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
   * Tests different types of {@link InputStream} and different sizes of the stream and ensures that the data is read
   * correctly.
   * @throws Exception
   */
  @Test
  public void commonCasesTest() throws Exception {
    int bufSize = InputStreamReadableStreamChannel.BUFFER_SIZE;
    int randSizeLessThanBuffer = TestUtils.RANDOM.nextInt(bufSize - 2) + 2;
    int randMultiplier = TestUtils.RANDOM.nextInt(10);
    int[] testStreamSizes =
        {0, 1, randSizeLessThanBuffer, bufSize, bufSize + 1, bufSize * randMultiplier, bufSize * randMultiplier + 1};
    for (int size : testStreamSizes) {
      byte[] src = TestUtils.getRandomBytes(size);

      InputStream stream = new ByteBufferInputStream(ByteBuffer.wrap(src));
      doReadTest(stream, src, src.length);

      stream = new ByteBufferInputStream(ByteBuffer.wrap(src));
      doReadTest(stream, src, -1);

      stream = new HaltingInputStream(new ByteBufferInputStream(ByteBuffer.wrap(src)));
      doReadTest(stream, src, src.length);

      stream = new HaltingInputStream(new ByteBufferInputStream(ByteBuffer.wrap(src)));
      doReadTest(stream, src, -1);
    }
  }

  /**
   * Verfies behavior of {@link InputStreamReadableStreamChannel#close()}.
   * @throws IOException
   */
  @Test
  public void closeTest() throws IOException {
    final AtomicBoolean streamOpen = new AtomicBoolean(true);
    InputStream stream = new InputStream() {
      @Override
      public int read() throws IOException {
        throw new IllegalStateException("Not implemented");
      }

      @Override
      public void close() {
        streamOpen.set(false);
      }
    };
    InputStreamReadableStreamChannel channel = new InputStreamReadableStreamChannel(stream, EXECUTOR_SERVICE);
    assertTrue("Channel should be open", channel.isOpen());
    channel.close();
    assertFalse("Channel should be closed", channel.isOpen());
    assertFalse("Stream should be closed", streamOpen.get());
    // close again is ok
    channel.close();
  }

  /**
   * Tests that the right exceptions are thrown when reading into {@link AsyncWritableChannel} fails.
   * @throws Exception
   */
  @Test
  public void readIntoAWCFailureTest() throws Exception {
    String errMsg = "@@ExpectedExceptionMessage@@";
    InputStream stream = new ByteBufferInputStream(ByteBuffer.allocate(1));

    // Bad AWC.
    InputStreamReadableStreamChannel channel = new InputStreamReadableStreamChannel(stream, EXECUTOR_SERVICE);
    ReadIntoCallback callback = new ReadIntoCallback();
    try {
      channel.readInto(new BadAsyncWritableChannel(new IOException(errMsg)), callback).get();
      fail("Should have failed because BadAsyncWritableChannel would have thrown exception");
    } catch (ExecutionException e) {
      Exception exception = (Exception) Utils.getRootCause(e);
      assertEquals("Exception message does not match expected (future)", errMsg, exception.getMessage());
      callback.awaitCallback();
      assertEquals("Exception message does not match expected (callback)", errMsg, callback.exception.getMessage());
    }

    // Read after close.
    channel = new InputStreamReadableStreamChannel(stream, EXECUTOR_SERVICE);
    channel.close();
    RetainingAsyncWritableChannel writeChannel = new RetainingAsyncWritableChannel();
    callback = new ReadIntoCallback();
    try {
      channel.readInto(writeChannel, callback).get();
      fail("InputStreamReadableStreamChannel has been closed, so read should have thrown ClosedChannelException");
    } catch (ExecutionException e) {
      Exception exception = (Exception) Utils.getRootCause(e);
      assertTrue("Exception is not ClosedChannelException", exception instanceof ClosedChannelException);
      callback.awaitCallback();
      assertEquals("Exceptions of callback and future differ", exception.getMessage(), callback.exception.getMessage());
    }

    // Reading more than once.
    channel = new InputStreamReadableStreamChannel(stream, EXECUTOR_SERVICE);
    writeChannel = new RetainingAsyncWritableChannel();
    channel.readInto(writeChannel, null);
    try {
      channel.readInto(writeChannel, null);
      fail("Should have failed because readInto cannot be called more than once");
    } catch (IllegalStateException e) {
      // expected. Nothing to do.
    }
  }

  /**
   * Tests that the right exceptions are thrown when the provided {@link InputStream} has unexpected behavior.
   * @throws Exception
   */
  @Test
  public void badInputStreamTest() throws Exception {
    final String errMsg = "@@ExpectedExceptionMessage@@";
    InputStream stream = new InputStream() {
      @Override
      public int read() throws IOException {
        // this represents any exception - bad behavior or closure before being read completely.
        throw new IllegalStateException(errMsg);
      }
    };

    InputStreamReadableStreamChannel channel = new InputStreamReadableStreamChannel(stream, EXECUTOR_SERVICE);
    ReadIntoCallback callback = new ReadIntoCallback();
    try {
      channel.readInto(new BadAsyncWritableChannel(new IOException(errMsg)), callback).get();
      fail("Should have failed because the InputStream would have thrown exception");
    } catch (ExecutionException e) {
      Exception exception = (Exception) Utils.getRootCause(e);
      assertEquals("Exception message does not match expected (future)", errMsg, exception.getMessage());
      callback.awaitCallback();
      assertEquals("Exception message does not match expected (callback)", errMsg, callback.exception.getMessage());
    }
  }

  // helpers
  // commonCasesTest() helpers

  /**
   * Does the test for reading from a {@link InputStreamReadableStreamChannel} that wraps the provided {@code stream}.
   * Ensures that the data used to construct the {@code stream} ({@code src}) matches the data that is read from the
   * created {@link InputStreamReadableStreamChannel}.
   * @param stream the {@link InputStream} to use.
   * @param src the data that was used to construct {@code stream}.
   * @param sizeToProvide the size to provide to the constructor of {@link InputStreamReadableStreamChannel}.
   * @throws Exception
   */
  private void doReadTest(InputStream stream, byte[] src, int sizeToProvide) throws Exception {
    InputStreamReadableStreamChannel channel;
    if (sizeToProvide >= 0) {
      channel = new InputStreamReadableStreamChannel(stream, sizeToProvide, EXECUTOR_SERVICE);
      assertEquals("Reported size of channel incorrect", sizeToProvide, channel.getSize());
    } else {
      channel = new InputStreamReadableStreamChannel(stream, EXECUTOR_SERVICE);
      assertEquals("Reported size of channel incorrect", -1, channel.getSize());
    }
    assertTrue("Channel should be open", channel.isOpen());
    RetainingAsyncWritableChannel writableChannel = new RetainingAsyncWritableChannel(src.length);
    ReadIntoCallback callback = new ReadIntoCallback();
    long bytesRead = channel.readInto(writableChannel, callback).get(1, TimeUnit.SECONDS);
    callback.awaitCallback();
    if (callback.exception != null) {
      throw callback.exception;
    }
    assertEquals("Total bytes written does not match (callback)", src.length, callback.bytesRead);
    assertEquals("Total bytes written does not match (future)", src.length, bytesRead);
    try (InputStream is = writableChannel.consumeContentAsInputStream()) {
      assertArrayEquals("Data does not match", src,
          Utils.readBytesFromStream(is, (int) writableChannel.getBytesWritten()));
    }
    channel.close();
    assertFalse("Channel should be closed", channel.isOpen());
    writableChannel.close();
  }
}

/**
 * Implementation of {@link InputStream} that sleeps before perfoming a read to simulate "blocking".
 * <p/>
 * Also reads only a fixed size on {@link #read(byte[], int, int)} to simulate partial data availability.
 */
class HaltingInputStream extends InputStream {
  private final InputStream stream;

  /**
   * Constructs a HaltingInputStream from the provided {@code stream}.
   * </p>
   * Sleeps before calling the read methods on the underlying stream. In the case of {@link #read(byte[], int, int)},
   * reads only the min of the length provided and a fixed size.
   * @param stream the {@link InputStream} to use to provide the actual data.
   */
  HaltingInputStream(InputStream stream) {
    this.stream = stream;
  }

  @Override
  public int read() throws IOException {
    sleep();
    return stream.read();
  }

  @Override
  public int read(byte b[], int off, int len) throws IOException {
    sleep();
    // simulate partial data availability.
    int lenToRead = Math.min(len, InputStreamReadableStreamChannel.BUFFER_SIZE / 2 - 1);
    return stream.read(b, off, lenToRead);
  }

  /**
   * Sleeps for a millisecond.
   */
  private void sleep() {
    try {
      Thread.sleep(1);
    } catch (InterruptedException e) {
      throw new IllegalStateException("Sleep was interrupted", e);
    }
  }
}
