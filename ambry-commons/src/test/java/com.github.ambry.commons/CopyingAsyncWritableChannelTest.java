/*
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.router.Callback;
import com.github.ambry.router.FutureResult;
import com.github.ambry.utils.TestUtils;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests for {@link CopyingAsyncWritableChannel}.
 */
public class CopyingAsyncWritableChannelTest {

  /**
   * Test that {@link CopyingAsyncWritableChannel} behaves as expected: chunks are copied, callback completed
   * immediately after {@link CopyingAsyncWritableChannel#write} method completes.
   */
  @Test
  public void basicsTest() throws Exception {
    List<byte[]> inputBuffers = getBuffers(1000, 20, 201, 0, 79, 1005);
    CopyingAsyncWritableChannel channel = new CopyingAsyncWritableChannel();
    for (int i = 0; i < inputBuffers.size(); i++) {
      ByteBuffer buf = ByteBuffer.wrap(inputBuffers.get(i));
      writeAndCheckCallback(buf, channel, buf.remaining(), null, null);
      checkStream(inputBuffers.subList(0, i + 1), channel);
    }
    channel.close();
    writeAndCheckCallback(ByteBuffer.allocate(0), channel, 0, ClosedChannelException.class, null);
  }

  /**
   * Ensure that buffers are copied and changes to the input buffers after a write call are not reflected in the
   * returned stream.
   */
  @Test
  public void bufferModificationTest() throws Exception {
    byte[] inputBuffer = TestUtils.getRandomBytes(100);
    CopyingAsyncWritableChannel channel = new CopyingAsyncWritableChannel();
    writeAndCheckCallback(ByteBuffer.wrap(inputBuffer), channel, inputBuffer.length, null, null);
    checkStream(Collections.singletonList(inputBuffer), channel);
    // mutate the input array and check that stream still matches the original content.
    byte[] originalBuffer = Arrays.copyOf(inputBuffer, inputBuffer.length);
    inputBuffer[50]++;
    checkStream(Collections.singletonList(originalBuffer), channel);
  }

  /**
   * Test that the size limit for bytes received is obeyed.
   */
  @Test
  public void sizeLimitTest() throws Exception {
    List<byte[]> inputBuffers = getBuffers(1000, 20, 5);
    CopyingAsyncWritableChannel channel = new CopyingAsyncWritableChannel(1023);
    for (Iterator<byte[]> iter = inputBuffers.iterator(); iter.hasNext(); ) {
      ByteBuffer buf = ByteBuffer.wrap(iter.next());
      if (iter.hasNext()) {
        writeAndCheckCallback(buf, channel, buf.remaining(), null, null);
      } else {
        writeAndCheckCallback(buf, channel, buf.remaining(), RestServiceException.class,
            RestServiceErrorCode.RequestTooLarge);
      }
    }
    // test that no more writes are accepted after size limit exceeded.
    writeAndCheckCallback(ByteBuffer.wrap(TestUtils.getRandomBytes(10)), channel, 0, RestServiceException.class,
        RestServiceErrorCode.RequestTooLarge);
  }

  /**
   * Get a list of byte arrays of the provided sizes.
   * @param chunkSizes the size in bytes for each chunk.
   * @return the list of buffers.
   */
  private static List<byte[]> getBuffers(int... chunkSizes) {
    return Arrays.stream(chunkSizes).mapToObj(TestUtils::getRandomBytes).collect(Collectors.toList());
  }

  /**
   * Test the behavior of {@link CopyingAsyncWritableChannel#write(ByteBuffer, Callback)}.
   * @param buf the buffer to write.
   * @param channel the channel to write to.
   * @param bytesWritten the expected number of bytes written.
   * @param exceptionClass if non-null, check that the write operation encountered this exception.
   * @param errorCode if non-null, check that the cause {@link RestServiceException} has this error code.
   */
  private static <E extends Exception> void writeAndCheckCallback(ByteBuffer buf, CopyingAsyncWritableChannel channel,
      long bytesWritten, Class<E> exceptionClass, RestServiceErrorCode errorCode) throws Exception {
    int remainingBeforeWrite = buf.remaining();
    FutureResult<Long> callbackResult = new FutureResult<>();
    FutureResult<Long> futureResult = (FutureResult<Long>) channel.write(buf, callbackResult::done);
    assertEquals("Unexpected number of bytes read from buffer", bytesWritten, remainingBeforeWrite - buf.remaining());
    for (FutureResult<Long> f : Arrays.asList(futureResult, callbackResult)) {
      // operation should be completed within method body.
      assertTrue("Operation not completed", f.isDone());
      assertEquals("Bytes written incorrect in callback", bytesWritten, (long) f.result());
      if (exceptionClass != null) {
        TestUtils.assertException(ExecutionException.class, f::get, e -> {
          E exceptionCause = exceptionClass.cast(e.getCause());
          if (errorCode != null && exceptionCause instanceof RestServiceException) {
            assertEquals("Unexpected error code", errorCode, ((RestServiceException) exceptionCause).getErrorCode());
          }
        });
      }
    }
  }

  /**
   * Check that the content in the stream returned by {@link CopyingAsyncWritableChannel#getContentAsInputStream()}
   * matches expectations.
   * @param expectedContent the expected content.
   * @param channel the channel that contains the copied content.
   */
  private static void checkStream(List<byte[]> expectedContent, CopyingAsyncWritableChannel channel) throws Exception {
    assertEquals("Bytes written does not match expected content length",
        expectedContent.stream().mapToLong(buf -> buf.length).sum(), channel.getBytesWritten());
    try (InputStream inputStream = channel.getContentAsInputStream()) {
      for (byte[] buf : expectedContent) {
        byte[] readBuf = new byte[buf.length];
        int read = inputStream.read(readBuf);
        assertEquals("Wrong number of bytes read", buf.length, read);
        assertArrayEquals("Read content should match expected", buf, readBuf);
      }
      assertEquals("Stream should be fully read", -1, inputStream.read());
    }
  }
}
