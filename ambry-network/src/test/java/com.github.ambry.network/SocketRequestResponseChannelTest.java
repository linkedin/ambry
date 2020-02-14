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

import com.github.ambry.utils.NettyByteBufLeakHelper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.WritableByteChannel;
import java.util.Random;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class SocketRequestResponseChannelTest {
  private final NettyByteBufLeakHelper nettyByteBufLeakHelper = new NettyByteBufLeakHelper();

  @Before
  public void before() {
    nettyByteBufLeakHelper.beforeTest();
  }

  @After
  public void after() {
    nettyByteBufLeakHelper.afterTest();
  }

  class ResponseListenerMock implements ResponseListener {
    public int call = 0;

    @Override
    public void onResponse(int processorId) {
      call++;
    }
  }

  class MockSend implements Send {
    public int sendcall = 1;

    @Override
    public long writeTo(WritableByteChannel channel) throws IOException {
      // no implementation
      return 0;
    }

    @Override
    public boolean isSendComplete() {
      return false;
    }

    @Override
    public long sizeInBytes() {
      return 10;
    }
  }

  @Test
  public void testSocketRequestResponseChannelTest() {
    try {
      SocketRequestResponseChannel channel = new SocketRequestResponseChannel(2, 10);
      Integer key = new Integer(5);
      String connectionId = "test_connectionId";
      ByteBuf buffer = PooledByteBufAllocator.DEFAULT.heapBuffer(1000);
      byte[] content = new byte[1000];
      new Random().nextBytes(content);
      buffer.writeBytes(content);
      SocketServerRequest request = new SocketServerRequest(0, connectionId, buffer);
      channel.sendRequest(request);
      request = (SocketServerRequest) channel.receiveRequest();
      Assert.assertEquals(request.getProcessor(), 0);
      Assert.assertEquals(request.getConnectionId(), connectionId);
      InputStream stream = request.getInputStream();
      for (int i = 0; i < 1000; i++) {
        Assert.assertEquals((byte) stream.read(), content[i]);
      }
      request.release();

      ResponseListenerMock mock = new ResponseListenerMock();
      channel.addResponseListener(mock);
      MockSend mocksend = new MockSend();
      channel.sendResponse(mocksend, request, null);
      Assert.assertEquals(mock.call, 1);
      SocketServerResponse response = (SocketServerResponse) channel.receiveResponse(0);
      Assert.assertEquals(response.getProcessor(), 0);
    } catch (Exception e) {
      Assert.assertEquals(true, false);
    }
  }
}
