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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Future;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests functionality of {@link ByteBufferReadableStreamChannel}.
 */
public class ByteBufferReadableStreamChannelTest {

  /**
   * Tests behavior of read operations on some corner cases.
   * <p/>
   * Corner case list:
   * 1. Blob size is 0.
   * @throws Exception
   */
  @Test
  public void readAndWriteCornerCasesTest() throws Exception {
    // 0 sized blob.
    ByteBufferReadableStreamChannel readableStreamChannel = new ByteBufferReadableStreamChannel(ByteBuffer.allocate(0));
    assertTrue("ByteBufferReadableStreamChannel is not open", readableStreamChannel.isOpen());
    assertEquals("Size returned by ByteBufferReadableStreamChannel is not 0", 0, readableStreamChannel.getSize());
    ByteBufferAsyncWritableChannel writeChannel = new ByteBufferAsyncWritableChannel();
    ReadIntoCallback callback = new ReadIntoCallback();
    Future<Long> future = readableStreamChannel.readInto(writeChannel, callback);
    ByteBuffer chunk = writeChannel.getNextChunk(0);
    while (chunk != null) {
      writeChannel.resolveOldestChunk(null);
      chunk = writeChannel.getNextChunk(0);
    }
    callback.awaitCallback();
    assertEquals("There should have no bytes to read (future)", 0, future.get().longValue());
    assertEquals("There should have no bytes to read (callback)", 0, callback.bytesRead);
    if (callback.exception != null) {
      throw callback.exception;
    }
    writeChannel.close();
    readableStreamChannel.close();
  }

  /**
   * Tests that the right exceptions are thrown when construction of {@link ByteBufferReadableStreamChannel} fails
   * <p/>
   * Corner case list:
   * 1. Create with a null {@link ByteBuffer}.
   * 2. Create with a null list of {@link ByteBuffer}.
   * 3. Create with an empty {@link ByteBuffer} list.
   * 4. Create with a list of {@link ByteBuffer} containing a null {@link ByteBuffer}.
   */
  @Test
  public void constructionErrorCasesTest() {
    // null byteBuffer
    try {
      new ByteBufferReadableStreamChannel((ByteBuffer) null);
      fail("Construction of ByteBufferReadableStreamChannel should have failed");
    } catch (RuntimeException e) {
      assertEquals("Unexpected RuntimeException", NullPointerException.class, e.getClass());
    }

    // null bufferList
    constructionFailureTest(null, IllegalArgumentException.class);

    // empty bufferList
    constructionFailureTest(Collections.EMPTY_LIST, IllegalArgumentException.class);

    // bufferList containing a null byteBuffer
    List<ByteBuffer> bufferList = Arrays.asList(ByteBuffer.wrap(fillRandomBytes(new byte[32])), null);
    Collections.shuffle(bufferList);
    constructionFailureTest(bufferList, NullPointerException.class);
  }

  // helpers
  // general

  /**
   * Calls the {@link ByteBufferReadableStreamChannel} constructor with the given {@code bufferList}
   * @param bufferList the list of {@link ByteBuffer} passed to the constructor
   * @param expectedExceptionType type of the thrown exception.
   */
  private void constructionFailureTest(List<ByteBuffer> bufferList, Class expectedExceptionType) {
    try {
      new ByteBufferReadableStreamChannel(bufferList);
      fail("Construction of ByteBufferReadableStreamChannel should have failed");
    } catch (RuntimeException e) {
      assertEquals("Unexpected RuntimeException", expectedExceptionType, e.getClass());
    }
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
