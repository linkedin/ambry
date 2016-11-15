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
import java.util.Random;
import org.junit.Assert;
import org.junit.Test;


/**
 * Test for CrcInputStream
 */
public class CrcInputStreamTest {
  @Test
  public void testCrcInputStream() throws IOException {
    byte[] buf = new byte[4000];
    new Random().nextBytes(buf);
    ByteBufferInputStream stream = new ByteBufferInputStream(ByteBuffer.wrap(buf));
    CrcInputStream crcStream = new CrcInputStream(stream);
    byte[] bufOutput = new byte[4000];
    try {
      crcStream.read(bufOutput);
    } catch (IOException e) {
      Assert.assertTrue(false);
    }
    Assert.assertArrayEquals(buf, bufOutput);
    long value1 = crcStream.getValue();
    stream = new ByteBufferInputStream(ByteBuffer.wrap(buf));
    crcStream = new CrcInputStream(stream);
    try {
      crcStream.read(bufOutput, 0, 3999);
      bufOutput[3999] = (byte) crcStream.read();
    } catch (IOException e) {
      Assert.assertTrue(false);
    }
    Assert.assertArrayEquals(buf, bufOutput);
    long value2 = crcStream.getValue();
    Assert.assertEquals(value1, value2);
    Assert.assertEquals(crcStream.available(), 0);
    buf[3999] = (byte) (~buf[3999]);
    stream = new ByteBufferInputStream(ByteBuffer.wrap(buf));
    crcStream = new CrcInputStream(stream);
    try {
      crcStream.read(bufOutput, 0, 3999);
      bufOutput[3999] = (byte) crcStream.read();
    } catch (IOException e) {
      Assert.assertTrue(false);
    }
    Assert.assertArrayEquals(buf, bufOutput);
    long value3 = crcStream.getValue();
    Assert.assertFalse(value2 == value3);
    crcStream.close();
  }
}
