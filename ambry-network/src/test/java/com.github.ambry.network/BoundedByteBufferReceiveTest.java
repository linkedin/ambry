package com.github.ambry.network;

import com.github.ambry.utils.ByteBufferInputStream;
import junit.framework.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.Random;


public class BoundedByteBufferReceiveTest {

  @Test
  public void testBoundedByteBufferReceive() {
    try {
      ByteBuffer buffer = ByteBuffer.allocate(2000);
      buffer.putLong(2000);
      byte[] buf = new byte[1992];
      new Random().nextBytes(buf);
      buffer.put(buf);
      buffer.flip();
      BoundedByteBufferReceive set = new BoundedByteBufferReceive();
      set.readFrom(Channels.newChannel(new ByteBufferInputStream(buffer)));
      buffer.clear();
      ByteBuffer payload = set.getPayload();
      for (int i = 8; i < 2000; i++) {
        Assert.assertEquals(buffer.array()[i], payload.get());
      }
    } catch (Exception e) {
      Assert.assertEquals(true, false);
    }
  }
}
