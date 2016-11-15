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
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Random;
import org.junit.Assert;
import org.junit.Test;


public class SocketRequestResponseChannelTest {

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
      ByteBuffer buffer = ByteBuffer.allocate(1000);
      new Random().nextBytes(buffer.array());
      channel.sendRequest(new SocketServerRequest(0, connectionId, new ByteBufferInputStream(buffer)));
      SocketServerRequest request = (SocketServerRequest) channel.receiveRequest();
      Assert.assertEquals(request.getProcessor(), 0);
      Assert.assertEquals(request.getConnectionId(), connectionId);
      InputStream stream = request.getInputStream();
      for (int i = 0; i < 1000; i++) {
        Assert.assertEquals((byte) stream.read(), buffer.array()[i]);
      }

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
