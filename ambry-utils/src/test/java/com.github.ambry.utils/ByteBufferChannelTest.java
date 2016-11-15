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
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;
import java.util.Random;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests functionality of {@link ByteBufferChannel}.
 */
public class ByteBufferChannelTest {

  /**
   * Tests the common case usage.
   * 1. Creates an input {@link ByteBuffer} with random data.
   * 2. Creates a {@link ByteBufferChannel} of capacity equal to the input buffer.
   * 3. Writes the data in the input buffer into the channel.
   * 4. Checks the bytes written (both number and data) match.
   * @throws ClosedChannelException
   */
  @Test
  public void commonCaseTest() throws ClosedChannelException {
    byte[] in = new byte[1024];
    byte[] out = new byte[1024];
    ByteBuffer inBuf = ByteBuffer.wrap(in);
    ByteBufferChannel channel = new ByteBufferChannel(ByteBuffer.wrap(out));
    ByteBuffer outBuf = channel.getBuffer();

    new Random().nextBytes(in);
    assertEquals("Bytes written is not as expected", inBuf.capacity(), channel.write(inBuf));
    assertArrayEquals("Bytes read from channel not equal to bytes written into channel", inBuf.array(), outBuf.array());
  }

  /**
   * Checks the case where the capacity of the source {@link ByteBuffer} and the capacity of {@link ByteBufferChannel}
   * being written to are unequal.
   * @throws ClosedChannelException
   */
  @Test
  public void unequalSrcChannelBufferTest() throws ClosedChannelException {
    sourceLargerThanChannelBufferTest();
    sourceSmallerThanChannelBufferTest();
  }

  /**
   * Checks the case where a {@link ByteBufferChannel} is used after it has been closed.
   * @throws ClosedChannelException
   */
  @Test
  public void useAfterCloseTest() throws ClosedChannelException {
    ByteBufferChannel channel = new ByteBufferChannel(ByteBuffer.allocate(5));
    channel.write(ByteBuffer.wrap(new byte[1]));
    channel.close();
    try {
      channel.write(ByteBuffer.wrap(new byte[1]));
    } catch (ClosedChannelException e) {
      // expected. nothing to do.
    }
  }

  // unequalSrcChannelBufferTest() helpers.
  private void sourceLargerThanChannelBufferTest() throws ClosedChannelException {
    byte[] in = new byte[2048];
    byte[] out = new byte[1024];
    ByteBuffer inBuf = ByteBuffer.wrap(in);
    ByteBufferChannel channel = new ByteBufferChannel(ByteBuffer.wrap(out));
    ByteBuffer outBuf = channel.getBuffer();

    new Random().nextBytes(in);
    assertEquals("Bytes written is not as expected", outBuf.capacity(), channel.write(inBuf));
    assertArrayEquals("Bytes read from channel not equal to bytes written into channel",
        Arrays.copyOfRange(inBuf.array(), 0, 1024), outBuf.array());
    assertEquals("No data should have been written into the channel", 0, channel.write(inBuf));

    outBuf.clear();
    assertEquals("Bytes written is not as expected", outBuf.capacity(), channel.write(inBuf));
    assertArrayEquals("Bytes read from channel not equal to bytes written into channel",
        Arrays.copyOfRange(inBuf.array(), 1024, 2048), outBuf.array());
    assertEquals("No data should have been written into the channel", 0, channel.write(inBuf));
  }

  private void sourceSmallerThanChannelBufferTest() throws ClosedChannelException {
    byte[] in = new byte[1024];
    byte[] out = new byte[2048];
    ByteBuffer inBuf = ByteBuffer.wrap(in);
    ByteBufferChannel channel = new ByteBufferChannel(ByteBuffer.wrap(out));
    ByteBuffer outBuf = channel.getBuffer();

    new Random().nextBytes(in);
    assertEquals("Bytes written is not as expected", inBuf.capacity(), channel.write(inBuf));
    assertArrayEquals("Bytes read from channel not equal to bytes written into channel", inBuf.array(),
        Arrays.copyOfRange(outBuf.array(), 0, 1024));
    assertEquals("No data should have been written into the channel", 0, channel.write(inBuf));

    new Random().nextBytes(in);
    inBuf.rewind();
    assertEquals("Bytes written is not as expected", inBuf.capacity(), channel.write(inBuf));
    assertArrayEquals("Bytes read from channel not equal to bytes written into channel", inBuf.array(),
        Arrays.copyOfRange(outBuf.array(), 1024, 2048));
    assertEquals("No data should have been written into the channel", 0, channel.write(inBuf));
  }
}

