package com.github.ambry.commons;

import com.github.ambry.router.Callback;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Unit tests for {@link ByteBufferAsyncWritableChannel}.
 */
public class ByteBufferAsyncWritableChannelTest {

  @Test
  public void commonCaseTest()
      throws Exception {
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
  public void checkoutMultipleChunksAndResolveTest()
      throws Exception {
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
  public void closeBeforeFullReadTest()
      throws Exception {
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
        Exception exception = getRootCause(e);
        assertTrue("Unexpected exception (future)", exception instanceof ClosedChannelException);
        assertTrue("Unexpected exception (callback)",
            writeData.writeCallback.exception instanceof ClosedChannelException);
      }
    }
  }

  @Test
  public void writeExceptionsTest()
      throws Exception {
    // null input.
    ByteBufferAsyncWritableChannel channel = new ByteBufferAsyncWritableChannel();
    try {
      channel.write(null, null);
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
      Exception exception = getRootCause(e);
      assertEquals("Unexpected exception message (future)", errMsg, exception.getMessage());
      assertEquals("Unexpected exception message (callback)", errMsg, writeCallback.exception.getMessage());
    }
  }

  /**
   * Checks the case where a {@link ByteBufferAsyncWritableChannel} is used after it has been closed.
   * @throws Exception
   */
  @Test
  public void useAfterCloseTest()
      throws Exception {
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
      Exception exception = getRootCause(e);
      assertTrue("Unexpected exception (future)", exception instanceof ClosedChannelException);
      assertTrue("Unexpected exception (callback)", writeCallback.exception instanceof ClosedChannelException);
    }

    // no chunks on getNextChunk()
    assertNull("There should have been no chunk returned", channel.getNextChunk());
    assertNull("There should have been no chunk returned", channel.getNextChunk(0));
  }

  // helpers
  // general

  /**
   * Gets the root cause for {@code e}.
   * @param e the {@link Exception} whose root cause is required.
   * @return the root cause for {@code e}.
   */
  private Exception getRootCause(Exception e) {
    Exception exception = e;
    while (exception.getCause() != null) {
      exception = (Exception) exception.getCause();
    }
    return exception;
  }

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

  public ChannelWriter(ByteBufferAsyncWritableChannel channel) {
    this.channel = channel;
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
      ByteBuffer chunk = ByteBuffer.wrap(data);
      Future<Long> future = channel.write(chunk, writeCallback);
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
