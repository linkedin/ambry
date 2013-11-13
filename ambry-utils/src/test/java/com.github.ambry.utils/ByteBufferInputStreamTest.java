package com.github.ambry.utils;


import junit.framework.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Random;
import java.io.IOException;

public class ByteBufferInputStreamTest {

  @Test
  public void ByteBufferStreamTest() throws IOException {
    byte[] buf = new byte[1024];
    new Random().nextBytes(buf);
    ByteBufferInputStream stream = new ByteBufferInputStream(ByteBuffer.wrap(buf));
    for (int i = 0; i < 1024; i++) {
      Assert.assertEquals(stream.read(), (buf[i] & 0xFF));
    }
    ByteBufferInputStream stream1 = new ByteBufferInputStream(ByteBuffer.wrap(buf));
    byte [] outputBuf = new byte[500];
    stream1.read(outputBuf, 0, 500);
    for (int i = 0; i < 500; i++)
      Assert.assertEquals(outputBuf[i], buf[i]);

    stream1.read(outputBuf, 0, 500);
    for (int i = 500; i < 1000; i++)
      Assert.assertEquals(outputBuf[i - 500], buf[i]);

  }
}
