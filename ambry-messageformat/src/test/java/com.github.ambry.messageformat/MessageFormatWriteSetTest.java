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
package com.github.ambry.messageformat;

import com.github.ambry.store.MessageInfo;
import com.github.ambry.store.Write;
import com.github.ambry.utils.ByteBufferInputStream;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;


public class MessageFormatWriteSetTest {

  public class MockWrite implements Write {

    ByteBuffer buf;

    public MockWrite(int size) {
      buf = ByteBuffer.allocate(size);
    }

    @Override
    public int appendFrom(ByteBuffer buffer) throws IOException {
      int toWrite = buffer.remaining();
      buf.put(buffer);
      return toWrite;
    }

    @Override
    public void appendFrom(ReadableByteChannel channel, long size) throws IOException {
      channel.read(buf);
    }

    public ByteBuffer getBuffer() {
      buf.flip();
      return buf;
    }
  }

  @Test
  public void writeSetTest() throws IOException {
    byte[] buf = new byte[2000];
    MessageInfo info1 = new MessageInfo(new MockId("id1"), 1000, 123, Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM), System.currentTimeMillis() + TestUtils.RANDOM.nextInt());
    MessageInfo info2 = new MessageInfo(new MockId("id2"), 1000, 123, Utils.getRandomShort(TestUtils.RANDOM),
        Utils.getRandomShort(TestUtils.RANDOM), System.currentTimeMillis() + TestUtils.RANDOM.nextInt());
    List<MessageInfo> infoList = new ArrayList<MessageInfo>();
    infoList.add(info1);
    infoList.add(info2);
    ByteBufferInputStream byteBufferInputStream = new ByteBufferInputStream(ByteBuffer.wrap(buf));
    MessageFormatWriteSet set = new MessageFormatWriteSet(byteBufferInputStream, infoList, false);
    MockWrite write = new MockWrite(2000);
    long written = set.writeTo(write);
    Assert.assertEquals(written, 2000);
    Assert.assertEquals(write.getBuffer().limit(), 2000);
    Assert.assertArrayEquals(write.getBuffer().array(), buf);
  }
}
