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
import com.github.ambry.router.ReadableStreamChannel;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


/**
 * Tests functionality of {@link ReadableStreamChannelInputStream}.
 */
public class ReadableStreamChannelInputStreamTest {

  /**
   * Tests the common cases i.e reading byte by byte, reading by parts and reading all at once.
   * @throws IOException
   */
  @Test
  public void commonCaseTest()
      throws IOException {
    byte[] in = new byte[1024];
    new Random().nextBytes(in);
    readByteByByteTest(in);
    readPartByPartTest(in);
    readAllAtOnceTest(in);
  }

  /**
   * Tests cases where read() methods get incorrect input.
   * @throws IOException
   */
  @Test
  public void readErrorCasesTest()
      throws IOException {
    byte[] in = new byte[1024];
    new Random().nextBytes(in);
    ReadableStreamChannel channel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(in));
    InputStream dstInputStream = new ReadableStreamChannelInputStream(channel);
    try {
      dstInputStream.read(null, 0, in.length);
      fail("The read should have failed");
    } catch (NullPointerException e) {
      // expected. nothing to do.
    }

    byte[] out = new byte[in.length];
    try {
      dstInputStream.read(out, -1, out.length);
      fail("The read should have failed");
    } catch (IndexOutOfBoundsException e) {
      // expected. nothing to do.
    }

    try {
      dstInputStream.read(out, 0, -1);
      fail("The read should have failed");
    } catch (IndexOutOfBoundsException e) {
      // expected. nothing to do.
    }

    try {
      dstInputStream.read(out, 0, out.length + 1);
      fail("The read should have failed");
    } catch (IndexOutOfBoundsException e) {
      // expected. nothing to do.
    }

    assertEquals("Bytes read should have been 0 because passed len was 0", 0, dstInputStream.read(out, 0, 0));
  }

  /**
   * Tests correctness of {@link ReadableStreamChannelInputStream#available()}.
   * @throws IOException
   */
  @Test
  public void availableTest()
      throws IOException {
    byte[] in = new byte[1024];
    new Random().nextBytes(in);

    ReadableStreamChannel channel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(in));
    InputStream stream = new ReadableStreamChannelInputStream(channel);
    doAvailableTest(stream, in);
    stream.close();

    channel = new NoSizeRSC(ByteBuffer.wrap(in));
    stream = new ReadableStreamChannelInputStream(channel);
    doAvailableTest(stream, in);
    stream.close();
  }

  /**
   * Tests for the case when reads are incomplete either because exceptions were thrown or the read simply did not
   * complete.
   * @throws IOException
   */
  @Test
  public void incompleteReadsTest()
      throws IOException {
    // Exception during read
    String exceptionMsg = "@@randomMsg@@";
    Exception exceptionToThrow = new Exception(exceptionMsg);
    ReadableStreamChannel channel = new IncompleteReadReadableStreamChannel(exceptionToThrow);
    InputStream inputStream = new ReadableStreamChannelInputStream(channel);
    try {
      inputStream.read();
      fail("The read should have failed");
    } catch (Exception e) {
      while (e.getCause() != null) {
        e = (Exception) e.getCause();
      }
      assertEquals("Exception messages do not match", exceptionMsg, e.getMessage());
    }

    // incomplete read
    channel = new IncompleteReadReadableStreamChannel(null);
    inputStream = new ReadableStreamChannelInputStream(channel);
    try {
      inputStream.read();
      fail("The read should have failed");
    } catch (IllegalStateException e) {
      // expected. Nothing to do.
    }
  }

  // helpers
  // commonCaseTest() helpers

  /**
   * Tests reading {@link ReadableStreamChannelInputStream} byte by byte.
   * @param in the data that the {@link ReadableStreamChannelInputStream} should contain.
   * @throws IOException
   */
  private void readByteByByteTest(byte[] in)
      throws IOException {
    ReadableStreamChannel channel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(in));
    InputStream stream = new ReadableStreamChannelInputStream(channel);
    doReadByteByByteTest(stream, in);
    stream.close();

    channel = new NoSizeRSC(ByteBuffer.wrap(in));
    stream = new ReadableStreamChannelInputStream(channel);
    doReadByteByByteTest(stream, in);
    stream.close();
  }

  /**
   * Tests reading {@link ReadableStreamChannelInputStream} part by part.
   * @param in the data that the {@link ReadableStreamChannelInputStream} should contain.
   * @throws IOException
   */
  private void readPartByPartTest(byte[] in)
      throws IOException {
    ReadableStreamChannel channel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(in));
    InputStream stream = new ReadableStreamChannelInputStream(channel);
    doReadPartByPartTest(stream, in);
    stream.close();

    channel = new NoSizeRSC(ByteBuffer.wrap(in));
    stream = new ReadableStreamChannelInputStream(channel);
    doReadPartByPartTest(stream, in);
    stream.close();
  }

  /**
   * Tests reading {@link ReadableStreamChannelInputStream} all at once.
   * @param in the data that the {@link ReadableStreamChannelInputStream} should contain.
   * @throws IOException
   */
  private void readAllAtOnceTest(byte[] in)
      throws IOException {
    ReadableStreamChannel channel = new ByteBufferReadableStreamChannel(ByteBuffer.wrap(in));
    InputStream stream = new ReadableStreamChannelInputStream(channel);
    doReadAllAtOnceTest(stream, in);
    stream.close();

    channel = new NoSizeRSC(ByteBuffer.wrap(in));
    stream = new ReadableStreamChannelInputStream(channel);
    doReadAllAtOnceTest(stream, in);
    stream.close();
  }

  /**
   * Tests reading {@link InputStream} byte by byte.
   * @param stream the {@link InputStream} to test.
   * @param in the original data that is inside {@code stream}.
   * @throws IOException
   */
  private void doReadByteByByteTest(InputStream stream, byte[] in)
      throws IOException {
    for (int i = 0; i < in.length; i++) {
      assertEquals("Byte [" + i + "] does not match expected", in[i], (byte) stream.read());
    }
    assertEquals("Did not receive expected EOF", -1, stream.read());
  }

  /**
   * Tests reading {@link InputStream} part by part.
   * @param stream the {@link InputStream} to test.
   * @param in the original data that is inside {@code stream}.
   * @throws IOException
   */
  private void doReadPartByPartTest(InputStream stream, byte[] in)
      throws IOException {
    byte[] out = new byte[in.length];
    for (int start = 0; start < in.length; ) {
      int end = Math.min(start + in.length / 4, in.length);
      int len = end - start;
      assertEquals("Bytes read did not match what was requested", len, stream.read(out, start, len));
      assertArrayEquals("Byte array obtained from InputStream did not match source", Arrays.copyOfRange(in, start, end),
          Arrays.copyOfRange(out, start, end));
      start = end;
    }
    assertEquals("Did not receive expected EOF", -1, stream.read(out, 0, out.length));
  }

  /**
   * Tests reading {@link InputStream} all at once.
   * @param stream the {@link InputStream} to test.
   * @param in the original data that is inside {@code stream}.
   * @throws IOException
   */
  private void doReadAllAtOnceTest(InputStream stream, byte[] in)
      throws IOException {
    byte[] out = new byte[in.length];
    assertEquals("Bytes read did not match size of source array", in.length, stream.read(out));
    assertArrayEquals("Byte array obtained from InputStream did not match source", in, out);
    assertEquals("Did not receive expected EOF", -1, stream.read(out));
  }

  /**
   * Tests correctness of {@link ReadableStreamChannelInputStream#available()}.
   * @param stream the {@link InputStream} to read from.
   * @param in the original data that is inside {@code stream}.
   * @throws IOException
   */
  private void doAvailableTest(InputStream stream, byte[] in)
      throws IOException {
    byte[] out = new byte[in.length / 5];
    int totalBytesRead = 0;
    for (int i = 0; totalBytesRead < in.length; i++) {
      int sourceStart = out.length * i;
      // available will be 0 when no chunks have been read.
      int expectedAvailable = sourceStart == 0 ? 0 :in.length - sourceStart;
      assertEquals("Available differs from expected", expectedAvailable, stream.available());
      int bytesRead = stream.read(out);
      assertArrayEquals("Byte array obtained from InputStream did not match source",
          Arrays.copyOfRange(in, sourceStart, sourceStart + bytesRead), Arrays.copyOfRange(out, 0, bytesRead));
      totalBytesRead += bytesRead;
    }
    assertEquals("Available should be 0", 0, stream.available());
  }
}

/**
 * {@link ReadableStreamChannel} implementation that either has an {@link Exception} on
 * {@link #readInto(AsyncWritableChannel, Callback)} or executes an incomplete read.
 */
class IncompleteReadReadableStreamChannel implements ReadableStreamChannel {
  private final AtomicBoolean channelOpen = new AtomicBoolean(true);
  private final Exception exceptionToThrow;

  /**
   * Create an instance of {@link IncompleteReadReadableStreamChannel} with an {@code exceptionToThrow}.
   * @param exceptionToThrow if desired, provide an exception that will thrown on read. Can be null.
   */
  public IncompleteReadReadableStreamChannel(Exception exceptionToThrow) {
    this.exceptionToThrow = exceptionToThrow;
  }

  @Override
  public long getSize() {
    return 1;
  }

  /**
   * Either throws the exception provided or returns immediately saying no bytes were read.
   * @param asyncWritableChannel the {@link AsyncWritableChannel} to read the data into.
   * @param callback the {@link Callback} that will be invoked either when all the data in the channel has been emptied
   *                 into the {@code asyncWritableChannel} or if there is an exception in doing so. This can be null.
   * @return a {@link Future} that will eventually contain the result of the operation.
   */
  @Override
  public Future<Long> readInto(AsyncWritableChannel asyncWritableChannel, Callback<Long> callback) {
    Exception exception;
    if (!channelOpen.get()) {
      exception = new ClosedChannelException();
    } else {
      exception = exceptionToThrow;
    }
    FutureResult<Long> futureResult = new FutureResult<Long>();
    futureResult.done(0L, exception);
    if (callback != null) {
      callback.onCompletion(0L, exception);
    }
    return futureResult;
  }

  @Override
  public boolean isOpen() {
    return channelOpen.get();
  }

  @Override
  public void close()
      throws IOException {
    channelOpen.set(false);
  }
}

/**
 * Implementation of {@link ReadableStreamChannel} that doesn't export the size.
 */
class NoSizeRSC extends ByteBufferReadableStreamChannel {

  NoSizeRSC(ByteBuffer buffer) {
    super(buffer);
  }

  /**
   * @return -1
   */
  @Override
  public long getSize() {
    return -1;
  }
}
