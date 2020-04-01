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
package com.github.ambry.protocol;

import com.github.ambry.network.Send;
import com.github.ambry.utils.ByteBufferOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.Assert;
import org.junit.Test;


class ByteArraySend implements Send {

  private ByteBuffer bytesToSend;

  public ByteArraySend(byte[] bytes) {
    this.bytesToSend = ByteBuffer.wrap(bytes);
  }

  @Override
  public long writeTo(WritableByteChannel channel) throws IOException {
    return channel.write(bytesToSend);
  }

  @Override
  public boolean isSendComplete() {
    return bytesToSend.remaining() == 0;
  }

  @Override
  public long sizeInBytes() {
    return bytesToSend.capacity();
  }
}

public class CompositeSendTest {

  @Test
  public void testCompositeSend() throws IOException {
    byte[] buf1 = new byte[1024];
    byte[] buf2 = new byte[2048];
    byte[] buf3 = new byte[4096];
    new Random().nextBytes(buf1);
    new Random().nextBytes(buf2);
    new Random().nextBytes(buf3);
    ByteArraySend byteArraySend1 = new ByteArraySend(buf1);
    ByteArraySend byteArraySend2 = new ByteArraySend(buf2);
    ByteArraySend byteArraySend3 = new ByteArraySend(buf3);
    List<Send> listToSend = new ArrayList<Send>(3);
    listToSend.add(byteArraySend1);
    listToSend.add(byteArraySend2);
    listToSend.add(byteArraySend3);
    CompositeSend compositeSend = new CompositeSend(listToSend);
    ByteBuffer bufferToWrite = ByteBuffer.allocate(1024 + 2048 + 4096);
    ByteBufferOutputStream bufferToWriteStream = new ByteBufferOutputStream(bufferToWrite);
    WritableByteChannel writableByteChannel = Channels.newChannel(bufferToWriteStream);
    while (!compositeSend.isSendComplete()) {
      compositeSend.writeTo(writableByteChannel);
    }
    bufferToWrite.flip();
    for (int i = 0; i < 1024; i++) {
      Assert.assertEquals(buf1[i], bufferToWrite.get(i));
    }
    for (int i = 0; i < 2048; i++) {
      Assert.assertEquals(buf2[i], bufferToWrite.get(1024 + i));
    }
    for (int i = 0; i < 4096; i++) {
      Assert.assertEquals(buf3[i], bufferToWrite.get(1024 + 2048 + i));
    }
  }
}
