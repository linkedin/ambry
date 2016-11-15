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
