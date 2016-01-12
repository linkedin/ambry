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
 * Unit tests for {@link ByteBufferScheduledWriteChannel}.
 */
public class ByteBufferScheduledWriteChannelTest {

  @Test
  public void commonCaseTest()
      throws Exception {
    ByteBufferScheduledWriteChannel channel = new ByteBufferScheduledWriteChannel();
    assertTrue("Channel is not open", channel.isOpen());
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
      channel.resolveChunk(chunk, null);
      assertEquals("Unexpected write size (future)", chunkSize, writeData.future.get().longValue());
      assertEquals("Unexpected write size (callback)", chunkSize, writeData.writeCallback.bytesWritten);
      chunkCount++;
      chunk = channel.getNextChunk(0);
    }
    assertEquals("Mismatch in number of chunks", channelWriter.writes.size(), chunkCount);
    channel.close();
    assertFalse("Channel is still open", channel.isOpen());
    assertNull("There should have been no chunk returned", channel.getNextChunk());
  }

  @Test
  public void closeBeforeFullReadTest()
      throws Exception {
    ByteBufferScheduledWriteChannel channel = new ByteBufferScheduledWriteChannel();
    assertTrue("Channel is not open", channel.isOpen());
    ChannelWriter channelWriter = new ChannelWriter(channel);
    channelWriter.writeToChannel(10);

    // read some chunks
    int i = 0;
    for (; i < 3; i++) {
      channel.resolveChunk(channel.getNextChunk(), null);
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
    ByteBufferScheduledWriteChannel channel = new ByteBufferScheduledWriteChannel();
    try {
      channel.write(null, null);
      fail("Write should have failed");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    // exception should be piped correctly.
    WriteCallback writeCallback = new WriteCallback();
    Future<Long> future = channel.write(ByteBuffer.allocate(1), writeCallback);
    String errMsg = "@@randomMsg@@";
    channel.resolveChunk(channel.getNextChunk(), new Exception(errMsg));

    try {
      future.get();
    } catch (ExecutionException e) {
      Exception exception = getRootCause(e);
      assertEquals("Unexpected exception message (future)", errMsg, exception.getMessage());
      assertEquals("Unexpected exception message (callback)", errMsg, writeCallback.exception.getMessage());
    }
  }

  @Test
  public void resolveChunkExceptionTest() {
    ByteBufferScheduledWriteChannel channel = new ByteBufferScheduledWriteChannel();
    ByteBuffer bogusChunk = ByteBuffer.allocate(5);
    // before any chunks are added.
    try {
      channel.resolveChunk(bogusChunk, null);
      fail("Resolving unknown chunks should throw exception");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }

    ByteBuffer validChunk = ByteBuffer.allocate(5);
    channel.write(validChunk, null);
    // resolving an unknown chunk.
    try {
      channel.resolveChunk(bogusChunk, null);
      fail("Resolving unknown chunks should throw exception");
    } catch (IllegalArgumentException e) {
      // expected. Nothing to do.
    }
    channel.close();
  }

  /**
   * Checks the case where a {@link ByteBufferScheduledWriteChannel} is used after it has been closed.
   * @throws Exception
   */
  @Test
  public void useAfterCloseTest()
      throws Exception {
    ByteBufferScheduledWriteChannel channel = new ByteBufferScheduledWriteChannel();
    channel.write(ByteBuffer.allocate(5), null);
    ByteBuffer chunk = channel.getNextChunk();
    channel.close();
    assertFalse("Channel is still open", channel.isOpen());

    // ok to close again
    channel.close();

    // ok to resolve chunk
    channel.resolveChunk(chunk, null);

    // not ok to write.
    WriteCallback writeCallback = new WriteCallback();
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
}

class ChannelWriter {
  public final List<WriteData> writes = new ArrayList<WriteData>();
  private final ByteBufferScheduledWriteChannel channel;
  private final Random random = new Random();

  public ChannelWriter(ByteBufferScheduledWriteChannel channel) {
    this.channel = channel;
  }

  public void writeToChannel(int writeCount) {
    for (int i = 0; i < writeCount; i++) {
      WriteCallback writeCallback = new WriteCallback();
      byte[] data = new byte[100];
      random.nextBytes(data);
      ByteBuffer chunk = ByteBuffer.wrap(data);
      Future<Long> future = channel.write(chunk, writeCallback);
      writes.add(new WriteData(data, future, writeCallback));
    }
  }
}

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
 * Callback for all write operations on {@link ByteBufferScheduledWriteChannel}.
 */
class WriteCallback implements Callback<Long> {
  public volatile long bytesWritten;
  public volatile Exception exception;
  private final AtomicBoolean callbackInvoked = new AtomicBoolean(false);

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
