package com.github.ambry.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import org.junit.Test;
import org.junit.Assert;


/**
 * Test for CrcInputStream
 */
public class CrcInputStreamTest {
  @Test
  public void testCrcInputStream()
      throws IOException {
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
    crcStream.close();
  }
}
