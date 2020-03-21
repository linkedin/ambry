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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.util.Random;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;


public class ByteBufferInputStreamTest {

  @Test
  public void byteBufferStreamTest() throws Exception {
    byte[] buf = new byte[1024];
    new Random().nextBytes(buf);
    ByteBufferInputStream stream = new ByteBufferInputStream(ByteBuffer.wrap(buf));
    for (int i = 0; i < 1024; i++) {
      Assert.assertEquals(stream.read(), (buf[i] & 0xFF));
    }
    ByteBufferInputStream stream1 = new ByteBufferInputStream(ByteBuffer.wrap(buf));
    byte[] outputBuf = new byte[500];
    stream1.read(outputBuf, 0, 500);
    for (int i = 0; i < 500; i++) {
      Assert.assertEquals(outputBuf[i], buf[i]);
    }

    stream1.read(outputBuf, 0, 500);
    for (int i = 500; i < 1000; i++) {
      Assert.assertEquals(outputBuf[i - 500], buf[i]);
    }
    ByteBufferInputStream stream2 = new ByteBufferInputStream(ByteBuffer.wrap(buf));
    ByteBufferInputStream stream3 = new ByteBufferInputStream(stream2, 1024);
    byte[] output = new byte[1001];
    output[0] = (byte) stream3.read();
    Assert.assertEquals(output[0], buf[0]);
    stream3.read(output, 1, 1000);
    for (int i = 0; i < 1001; i++) {
      Assert.assertEquals(output[i], buf[i]);
    }
    output = new byte[23];
    stream3.read(output);
    for (int i = 0; i < 23; i++) {
      Assert.assertEquals(output[i], buf[i + 1001]);
    }
    Assert.assertEquals(-1, stream3.read());
    TestUtils.validateInputStreamContract(stream3);

    ByteBuffer byteBuf = ByteBuffer.wrap(buf);
    ByteBufferInputStream stream4 = new ByteBufferInputStream(byteBuf.duplicate());
    // ByteBuffer class overrides equal() to do content comparison.
    Assert.assertEquals("The returned byte buffer must have the same content as the one initialized with", byteBuf,
        stream4.getByteBuffer());
    byteBuf.rewind();
    ByteBufferInputStream stream5 = new ByteBufferInputStream(byteBuf.duplicate());
    ByteBufferInputStream stream6 = new ByteBufferInputStream(stream5, 1024);
    Assert.assertEquals("The returned byte buffer must have the same content as the one initialized with", byteBuf,
        stream6.getByteBuffer());

    try {
      stream6.getByteBuffer().put((byte) 0);
      Assert.fail("Returned ByteBuffer from a ByteBufferInputStream must be read-only");
    } catch (ReadOnlyBufferException e) {
    }
  }

  @Test
  public void markResetTest() throws IOException {
    byte[] buf = new byte[1024];
    new Random().nextBytes(buf);

    // Common case use
    ByteBufferInputStream stream = new ByteBufferInputStream(ByteBuffer.wrap(buf));
    assertTrue(stream.markSupported());
    stream.mark(1024);
    for (int i = 0; i < 1024; i++) {
      Assert.assertEquals(stream.read(), (buf[i] & 0xFF));
    }
    stream.reset();
    for (int i = 0; i < 1024; i++) {
      Assert.assertEquals(stream.read(), (buf[i] & 0xFF));
    }

    // Expect exception on reset afer reading beyond readLimit
    ByteBufferInputStream stream2 = new ByteBufferInputStream(ByteBuffer.wrap(buf));
    stream2.mark(1023);
    for (int i = 0; i < 1024; i++) {
      Assert.assertEquals(stream2.read(), (buf[i] & 0xFF));
    }
    try {
      stream2.reset();
      fail("stream reset should have thrown.");
    } catch (IOException e) {
      // Expected
    }

    // Expect exception on reset without mark being called.
    ByteBufferInputStream stream3 = new ByteBufferInputStream(ByteBuffer.wrap(buf));
    try {
      stream3.reset();
      fail("stream reset should have thrown.");
    } catch (IOException e) {
      // Expected
    }
  }
}
