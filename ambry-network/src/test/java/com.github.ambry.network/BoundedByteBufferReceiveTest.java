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
package com.github.ambry.network;

import com.github.ambry.utils.ByteBufferInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.Random;
import org.junit.Assert;
import org.junit.Test;


public class BoundedByteBufferReceiveTest {

  /**
   * Test basic operation of {@link BoundedByteBufferReceive}.
   * @throws Exception
   */
  @Test
  public void testBoundedByteBufferReceive() throws Exception {
    int bufferSize = 2000;
    ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
    buffer.putLong(bufferSize);
    byte[] buf = new byte[bufferSize - Long.BYTES];
    new Random().nextBytes(buf);
    buffer.put(buf);
    buffer.flip();
    BoundedByteBufferReceive set = new BoundedByteBufferReceive();
    Assert.assertEquals("Wrong number of bytes read", bufferSize,
        set.readFrom(Channels.newChannel(new ByteBufferInputStream(buffer))));
    buffer.clear();
    ByteBuffer payload = set.getPayload();
    for (int i = 8; i < bufferSize; i++) {
      Assert.assertEquals(buffer.array()[i], payload.get());
    }
  }
}
