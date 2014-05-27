package com.github.ambry.network;

import com.github.ambry.utils.ByteBufferInputStream;
import junit.framework.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.Random;


public class SocketServerInputSetTest {

  @Test
  public void testSocketServerInputSet() {
    try {
      ByteBuffer buffer = ByteBuffer.allocate(2000);
      buffer.putLong(2000);
      byte[] buf = new byte[1992];
      new Random().nextBytes(buf);
      buffer.put(buf);
      buffer.flip();
      SocketServerInputSet set = new SocketServerInputSet();
      set.readFrom(Channels.newChannel(new ByteBufferInputStream(buffer)));
      buffer.clear();
      for (int i = 8; i < 2000; i++) {
        Assert.assertEquals(buffer.array()[i], (byte) set.read());
      }
    } catch (Exception e) {
      Assert.assertEquals(true, false);
    }
  }
}
