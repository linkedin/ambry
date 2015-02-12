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
  public void testCrcOutputStream()
      throws IOException {
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
    crcStream.close();
  }
}
