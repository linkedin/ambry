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
package com.github.ambry.commons;

import com.github.ambry.router.Callback;
import com.github.ambry.utils.NettyByteBufLeakHelper;
import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Unit tests for {@link ByteBufferAsyncWritableChannel}.
 */
public class ByteBufferAsyncWritableChannelTest {

  private final NettyByteBufLeakHelper nettyByteBufLeakHelper = new NettyByteBufLeakHelper();

  @Before
  public void before() {
    nettyByteBufLeakHelper.beforeTest();
  }

  @After
  public void after() {
    nettyByteBufLeakHelper.afterTest();
  }

  @Test
  public void commonCaseTest() throws Exception {
    ByteBufferAsyncWritableChannel channel = new ByteBufferAsyncWritableChannel();
    assertTrue("Channel is not open", channel.isOpen());
    assertNull("There should have been no chunk returned", channel.getNextChunk(0));
    ChannelWriter channelWriter = new ChannelWriter(channel);
    channelWriter.writeToChannel(10);

    int chunkCount = 0;
    ByteBuffer chunk = channel.getNextChunk();
    while (chunk != null) {
      WriteData writeData = channelWriter.writes.get(chunkCount);
      int chunkSize = chunk.remaining();
      byte[] writtenChunk = writeData.writtenChunk;
      byte[] readChunk = new byte[writtenChunk.length];
      chunk.get(readChunk);
      assertArrayEquals("Data unequal", writtenChunk, readChunk);
      channel.resolveOldestChunk(null);
      assertEquals("Unexpected write size (future)", chunkSize, writeData.future.get().longValue());
      assertEquals("Unexpected write size (callback)", chunkSize, writeData.writeCallback.bytesWritten);
      chunkCount++;
      chunk = channel.getNextChunk(0);
    }
    assertEquals("Mismatch in number of chunks", channelWriter.writes.size(), chunkCount);
    channel.close();
    assertFalse("Channel is still open", channel.isOpen());
    assertNull("There should have been no chunk returned", channel.getNextChunk());
    assertNull("There should have been no chunk returned", channel.getNextChunk(0));
  }

  @Test
  public void commonCaseTestForNettyByteBuf() throws Exception {
    for (boolean useCompositeByteBuf : Arrays.asList(false, true)) {
      ByteBufferAsyncWritableChannel channel = new ByteBufferAsyncWritableChannel();
      assertTrue("Channel is not open", channel.isOpen());
      assertNull("There should have been no chunk returned", channel.getNextChunk(0));
      ChannelWriter channelWriter = new ChannelWriter(channel, true, useCompositeByteBuf);
      channelWriter.writeToChannel(10);

      int chunkCount = 0;
      ByteBuf chunk = channel.getNextByteBuf();
      while (chunk != null) {
        WriteData writeData = channelWriter.writes.get(chunkCount);
        int chunkSize = chunk.readableBytes();
        byte[] writtenChunk = writeData.writtenChunk;
        byte[] readChunk = new byte[writtenChunk.length];
        chunk.readBytes(readChunk);
        assertArrayEquals("Data unequal", writtenChunk, readChunk);
        channel.resolveOldestChunk(null);
        assertEquals("Unexpected write size (future)", chunkSize, writeData.future.get().longValue());
        assertEquals("Unexpected write size (callback)", chunkSize, writeData.writeCallback.bytesWritten);
        chunkCount++;
        chunk = channel.getNextByteBuf(0);
      }
      assertEquals("Mismatch in number of ByteBufs", channelWriter.writes.size(), chunkCount);
      channel.close();
      assertFalse("Channel is still open", channel.isOpen());
      assertNull("There should have been no ByteBuf returned", channel.getNextByteBuf());
      assertNull("There should have been no ByteBuf returned", channel.getNextByteBuf(0));
    }
  }

  @Test
  public void checkoutMultipleChunksAndResolveTest() throws Exception {
    ByteBufferAsyncWritableChannel channel = new ByteBufferAsyncWritableChannel();
    ChannelWriter channelWriter = new ChannelWriter(channel);
    channelWriter.writeToChannel(5);

    // get all chunks without resolving any
    ByteBuffer chunk = channel.getNextChunk(0);
    while (chunk != null) {
      chunk = channel.getNextChunk(0);
    }

    // now resolve them one by one and check that ordering is respected.
    for (int i = 0; i < channelWriter.writes.size(); i++) {
      channel.resolveOldestChunk(null);
      ensureCallbackOrder(channelWriter, i);
    }
    channel.close();
  }

  @Test
  public void closeBeforeFullReadTest() throws Exception {
    ByteBufferAsyncWritableChannel channel = new ByteBufferAsyncWritableChannel();
    assertTrue("Channel is not open", channel.isOpen());
    ChannelWriter channelWriter = new ChannelWriter(channel);
    channelWriter.writeToChannel(10);

    // read some chunks
    int i = 0;
    for (; i < 3; i++) {
      channel.getNextChunk(0);
      channel.resolveOldestChunk(null);
    }
    channel.close();
    assertFalse("Channel is still open", channel.isOpen());
    for (; i < channelWriter.writes.size(); i++) {
      WriteData writeData = channelWriter.writes.get(i);
      try {
        writeData.future.get();
      } catch (ExecutionException e) {
        Exception exception = (Exception) Utils.getRootCause(e);
        assertTrue("Unexpected exception (future)", exception instanceof ClosedChannelException);
        assertTrue("Unexpected exception (callback)",
            writeData.writeCallback.exception instanceof ClosedChannelException);
      }
    }
  }

  @Test
  public void writeExceptionsTest() throws Exception {
    // null input.
    ByteBufferAsyncWritableChannel channel = new ByteBufferAsyncWritableChannel();
    try {
      channel.write((ByteBuf) null, null);
      fail("Write should have failed");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    // exception should be piped correctly.
    WriteCallback writeCallback = new WriteCallback(0);
    Future<Long> future = channel.write(ByteBuffer.allocate(1), writeCallback);
    String errMsg = "@@randomMsg@@";
    channel.getNextChunk(0);
    channel.resolveOldestChunk(new Exception(errMsg));

    try {
      future.get();
    } catch (ExecutionException e) {
      Exception exception = (Exception) Utils.getRootCause(e);
      assertEquals("Unexpected exception message (future)", errMsg, exception.getMessage());
      assertEquals("Unexpected exception message (callback)", errMsg, writeCallback.exception.getMessage());
    }
  }

  /**
   * Checks the case where a {@link ByteBufferAsyncWritableChannel} is used after it has been closed.
   * @throws Exception
   */
  @Test
  public void useAfterCloseTest() throws Exception {
    ByteBufferAsyncWritableChannel channel = new ByteBufferAsyncWritableChannel();
    channel.write(ByteBuffer.allocate(5), null);
    channel.getNextChunk();
    channel.close();
    assertFalse("Channel is still open", channel.isOpen());

    // ok to close again
    channel.close();

    // ok to resolve chunk
    channel.resolveOldestChunk(null);

    // not ok to write.
    WriteCallback writeCallback = new WriteCallback(0);
    try {
      channel.write(ByteBuffer.allocate(0), writeCallback).get();
      fail("Write should have failed");
    } catch (ExecutionException e) {
      Exception exception = (Exception) Utils.getRootCause(e);
      assertTrue("Unexpected exception (future)", exception instanceof ClosedChannelException);
      assertTrue("Unexpected exception (callback)", writeCallback.exception instanceof ClosedChannelException);
    }

    // no chunks on getNextChunk()
    assertNull("There should have been no chunk returned", channel.getNextChunk());
    assertNull("There should have been no chunk returned", channel.getNextChunk(0));
  }

  /**
   * Test to verify notification for all channel events.
   */
  @Test
  public void testChannelEventNotification() throws Exception {
    final AtomicBoolean writeNotified = new AtomicBoolean(false);
    final AtomicBoolean closeNotified = new AtomicBoolean(false);
    ByteBufferAsyncWritableChannel channel =
        new ByteBufferAsyncWritableChannel(new ByteBufferAsyncWritableChannel.ChannelEventListener() {
          @Override
          public void onEvent(ByteBufferAsyncWritableChannel.EventType e) {
            if (e == ByteBufferAsyncWritableChannel.EventType.Write) {
              writeNotified.set(true);
            } else if (e == ByteBufferAsyncWritableChannel.EventType.Close) {
              closeNotified.set(true);
            }
          }
        });
    assertFalse("No write notification should have come in before any write", writeNotified.get());
    channel.write(ByteBuffer.allocate(5), null);
    assertTrue("Write should have been notified", writeNotified.get());
    assertFalse("No close event notification should have come in before a close", closeNotified.get());
    channel.close();
    assertTrue("Close should have been notified", closeNotified.get());
  }

  // helpers

  // checkoutMultipleChunksAndResolveTest() helpers.

  /**
   * Ensures that the callback with {@code expectedCallbackId} is invoked but callbacks for chunks younger than the
   * expected one (i.e. id > {@code expectedCallbackId}) have not been invoked.
   * @param channelWriter the {@link ChannelWriter} that wrote to the channel.
   * @param expectedCallbackId the id of the callback expected to be invoked.
   */
  private void ensureCallbackOrder(ChannelWriter channelWriter, int expectedCallbackId) {
    WriteCallback writeCallback = channelWriter.writes.get(expectedCallbackId).writeCallback;
    assertTrue("Callback for the expected oldest chunk not invoked", writeCallback.callbackInvoked.get());
    for (int i = expectedCallbackId + 1; i < channelWriter.writes.size(); i++) {
      writeCallback = channelWriter.writes.get(i).writeCallback;
      assertFalse("Callback for a chunk younger than the expected is invoked", writeCallback.callbackInvoked.get());
    }
  }
}

/**
 * Writes some random data to the provided {@link ByteBufferAsyncWritableChannel}.
 */
class ChannelWriter {
  public final List<WriteData> writes = new ArrayList<WriteData>();
  private final ByteBufferAsyncWritableChannel channel;
  private final Random random = new Random();
  private final boolean useNettyByteBuf;
  private final boolean useCompositeByteBuf;

  public ChannelWriter(ByteBufferAsyncWritableChannel channel) {
    this(channel, false, false);
  }

  public ChannelWriter(ByteBufferAsyncWritableChannel channel, boolean useNettyByteBuf) {
    this(channel, useNettyByteBuf, false);
  }

  public ChannelWriter(ByteBufferAsyncWritableChannel channel, boolean useNettyByteBuf, boolean useCompositeByteBuf) {
    this.channel = channel;
    this.useNettyByteBuf = useNettyByteBuf;
    this.useCompositeByteBuf = useCompositeByteBuf;
  }

  /**
   * Writes {@code writeCount} number of random chunks to the given {@link ByteBufferAsyncWritableChannel}.
   * @param writeCount the number of chunks to write.
   */
  public void writeToChannel(int writeCount) {
    for (int i = 0; i < writeCount; i++) {
      WriteCallback writeCallback = new WriteCallback(i);
      byte[] data = new byte[100];
      random.nextBytes(data);
      Future<Long> future = null;
      if (useNettyByteBuf) {
        ByteBuf chunk = null;
        if (!useCompositeByteBuf) {
          chunk = ByteBufAllocator.DEFAULT.heapBuffer(data.length);
          chunk.writeBytes(data);
        } else {
          CompositeByteBuf composite = ByteBufAllocator.DEFAULT.compositeHeapBuffer(100);
          ByteBuf c = ByteBufAllocator.DEFAULT.heapBuffer(50);
          c.writeBytes(data, 0, 50);
          composite.addComponent(true, c);
          c = ByteBufAllocator.DEFAULT.heapBuffer(50);
          c.writeBytes(data, 50, 50);
          composite.addComponent(true, c);
          chunk = composite;
        }
        final ByteBuf finalByteBuf = chunk;
        future = channel.write(finalByteBuf, (result, exception) -> {
          finalByteBuf.release();
          writeCallback.onCompletion(result, exception);
        });
      } else {
        ByteBuffer chunk = ByteBuffer.wrap(data);
        future = channel.write(chunk, writeCallback);
      }
      writes.add(new WriteData(data, future, writeCallback));
    }
  }
}

/**
 * Represents the data associated with a write.
 */
class WriteData {
  public final byte[] writtenChunk;
  public final Future<Long> future;
  public final WriteCallback writeCallback;

  public WriteData(byte[] writtenChunk, Future<Long> future, WriteCallback writeCallback) {
    this.writtenChunk = writtenChunk;
    this.future = future;
    this.writeCallback = writeCallback;
  }
}

/**
 * Callback for all write operations on {@link ByteBufferAsyncWritableChannel}.
 */
class WriteCallback implements Callback<Long> {
  public volatile int writeId;
  public volatile long bytesWritten;
  public volatile Exception exception;
  public final AtomicBoolean callbackInvoked = new AtomicBoolean(false);

  /**
   * Create a write callback.
   * @param writeId the id to attach to the callback.
   */
  public WriteCallback(int writeId) {
    this.writeId = writeId;
  }

  @Override
  public void onCompletion(Long result, Exception exception) {
    if (callbackInvoked.compareAndSet(false, true)) {
      bytesWritten = result;
      this.exception = exception;
    } else {
      this.exception = new IllegalStateException("Callback invoked more than once");
    }
  }
}
