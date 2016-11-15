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
 * Tests for CrcOutputStream
 */
public class CrcOutputStreamTest {
  @Test
  public void testCrcOutputStream() throws IOException {
    byte[] buf = new byte[4000];
    ByteBufferOutputStream stream = new ByteBufferOutputStream(ByteBuffer.wrap(buf));
    CrcOutputStream crcStream = new CrcOutputStream(stream);
    byte[] bufOutput = new byte[4000];
    new Random().nextBytes(bufOutput);
    try {
      crcStream.write(bufOutput);
    } catch (IOException e) {
      Assert.assertTrue(false);
    }
    Assert.assertArrayEquals(buf, bufOutput);
    long value1 = crcStream.getValue();
    stream = new ByteBufferOutputStream(ByteBuffer.wrap(buf));
    crcStream = new CrcOutputStream(stream);
    try {
      crcStream.write(bufOutput[0]);
      crcStream.write(bufOutput, 1, 3999);
    } catch (IOException e) {
      Assert.assertTrue(false);
    }
    Assert.assertArrayEquals(buf, bufOutput);
    long value2 = crcStream.getValue();
    Assert.assertEquals(value1, value2);
    bufOutput[0] = (byte) (~bufOutput[0]);
    stream = new ByteBufferOutputStream(ByteBuffer.wrap(buf));
    crcStream = new CrcOutputStream(stream);
    try {
      crcStream.write(bufOutput[0]);
      crcStream.write(bufOutput, 1, 3999);
    } catch (IOException e) {
      Assert.assertTrue(false);
    }
    Assert.assertArrayEquals(buf, bufOutput);
    long value3 = crcStream.getValue();
    Assert.assertFalse(value2 == value3);
    crcStream.close();
  }
}
