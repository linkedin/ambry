/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.utils.NettyByteBufLeakHelper;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.Random;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class BoundedNettyByteBufReceiveTest {

  private final NettyByteBufLeakHelper nettyByteBufLeakHelper = new NettyByteBufLeakHelper();

  @Before
  public void before() {
    nettyByteBufLeakHelper.beforeTest();
  }

  @After
  public void after() {
    nettyByteBufLeakHelper.afterTest();
  }

  /**
   * Test basic operation of {@link BoundedNettyByteBufReceive}.
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
    BoundedNettyByteBufReceive set = new BoundedNettyByteBufReceive(100000);
    Assert.assertEquals("Wrong number of bytes read", bufferSize,
        set.readFrom(Channels.newChannel(new ByteBufferInputStream(buffer))));
    buffer.clear();
    ByteBuf payload = set.content();
    for (int i = 8; i < bufferSize; i++) {
      Assert.assertEquals(buffer.array()[i], payload.readByte());
    }
    payload.release();
  }

  /**
   * Test when the request size is bigger than the maximum size
   * @throws Exception
   */
  @Test
  public void testBoundedByteBufferReceiveOnLargeRequest() throws Exception {
    int bufferSize = 2000;
    ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
    buffer.putLong(bufferSize);
    byte[] buf = new byte[bufferSize - Long.BYTES];
    new Random().nextBytes(buf);
    buffer.put(buf);
    buffer.flip();
    BoundedNettyByteBufReceive set = new BoundedNettyByteBufReceive(100);
    // The max request size is 100, but the buffer size is 2000, will result in IOException
    try {
      set.readFrom(Channels.newChannel(new ByteBufferInputStream(buffer)));
      Assert.fail("Should fail with IOException");
    } catch (IOException e) {
    }
    buffer.clear();
  }

  /**
   * Regression test for ByteBuf pool leak when IOException occurs during content read.
   * Verifies that the intermediate buffer approach correctly handles exceptions without leaking ByteBufs.
   *
   * This test specifically validates the fix for a bug where using nioBuffer() caused pool
   * deallocation issues when exceptions occurred during channel.read(). The fix uses an
   * intermediate ByteBuffer to completely decouple the channel read from ByteBuf internal state.
   *
   * @throws Exception
   */
  @Test
  public void testIOExceptionDuringContentReadNoLeak() throws Exception {
    int requestSize = 100;

    // Channel that returns size, then 0, then throws IOException
    // This forces readFrom() to be called twice: once for size, once for content
    java.nio.channels.ReadableByteChannel faultingChannel = new java.nio.channels.ReadableByteChannel() {
      private int readCount = 0;

      @Override
      public int read(ByteBuffer dst) throws IOException {
        if (readCount == 0) {
          dst.putLong(requestSize);
          readCount++;
          return Long.BYTES;
        } else if (readCount == 1) {
          // Return 0 to force readFrom() to return before reading content
          readCount++;
          return 0;
        } else {
          throw new IOException("Simulated network error during content read");
        }
      }

      @Override
      public boolean isOpen() {
        return true;
      }

      @Override
      public void close() throws IOException {
      }
    };

    BoundedNettyByteBufReceive receive = new BoundedNettyByteBufReceive(100000);

    // First call reads size successfully
    long bytesRead = receive.readFrom(faultingChannel);
    Assert.assertEquals("Should have read size header", Long.BYTES, bytesRead);

    // Second call should throw IOException during content read
    try {
      receive.readFrom(faultingChannel);
      Assert.fail("Should have thrown IOException");
    } catch (IOException e) {
      Assert.assertTrue("Exception should mention content read error",
          e.getMessage().contains("content read"));
    }

    // NettyByteBufLeakHelper.afterTest() will verify no leaks
  }

  /**
   * Regression test for ByteBuf pool leak when EOFException occurs during content read.
   * Verifies that the intermediate buffer approach correctly handles EOF without leaking ByteBufs.
   *
   * @throws Exception
   */
  @Test
  public void testEOFExceptionDuringContentReadNoLeak() throws Exception {
    int requestSize = 100;

    // Channel that returns size, then 0, then EOF
    java.nio.channels.ReadableByteChannel eofChannel = new java.nio.channels.ReadableByteChannel() {
      private int readCount = 0;

      @Override
      public int read(ByteBuffer dst) throws IOException {
        if (readCount == 0) {
          dst.putLong(requestSize);
          readCount++;
          return Long.BYTES;
        } else if (readCount == 1) {
          // Return 0 to force readFrom() to return before reading content
          readCount++;
          return 0;
        }
        return -1; // EOF during content read
      }

      @Override
      public boolean isOpen() {
        return true;
      }

      @Override
      public void close() throws IOException {
      }
    };

    BoundedNettyByteBufReceive receive = new BoundedNettyByteBufReceive(100000);

    // First call reads size successfully
    long bytesRead = receive.readFrom(eofChannel);
    Assert.assertEquals("Should have read size header", Long.BYTES, bytesRead);

    // Second call should throw EOFException during content read
    try {
      receive.readFrom(eofChannel);
      Assert.fail("Should have thrown EOFException");
    } catch (IOException e) {
      // Expected
    }

    // NettyByteBufLeakHelper.afterTest() will verify no leaks
  }
}
