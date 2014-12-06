package com.github.ambry.messageformat;

import com.github.ambry.store.MessageInfo;
import com.github.ambry.utils.ByteBufferInputStream;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;

import com.github.ambry.store.Write;


public class MessageFormatWriteSetTest {

  public class MockWrite implements Write {

    ByteBuffer buf;

    public MockWrite(int size) {
      buf = ByteBuffer.allocate(size);
    }

    @Override
    public int appendFrom(ByteBuffer buffer)
        throws IOException {
      int toWrite = buffer.remaining();
      buf.put(buffer);
      return toWrite;
    }

    @Override
    public void appendFrom(ReadableByteChannel channel, long size)
        throws IOException {
      channel.read(buf);
    }

    public ByteBuffer getBuffer() {
      buf.flip();
      return buf;
    }
  }

  @Test
  public void writeSetTest()
      throws IOException {
    byte[] buf = new byte[2000];
    MessageInfo info1 = new MessageInfo(new MessageFormatInputStreamTest.MockId("id1"), 1000, 123);
    MessageInfo info2 = new MessageInfo(new MessageFormatInputStreamTest.MockId("id2"), 1000, 123);
    List<MessageInfo> infoList = new ArrayList<MessageInfo>();
    infoList.add(info1);
    infoList.add(info2);
    MessageFormatWriteSet set =
        new MessageFormatWriteSet(new ByteBufferInputStream(ByteBuffer.wrap(buf)), infoList, false);
    MockWrite write = new MockWrite(2000);
    long written = set.writeTo(write);
    Assert.assertEquals(written, 2000);
    Assert.assertEquals(write.getBuffer().limit(), 2000);
    Assert.assertArrayEquals(write.getBuffer().array(), buf);
  }
}
