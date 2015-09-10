package com.github.ambry.router;

import com.github.ambry.utils.ByteBufferChannel;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * Tests functionality of {@link ByteBufferReadableStreamChannel}.
 */
public class ByteBufferReadableStreamChannelTest {

  /**
   * Tests the common case read operations i.e
   * 1. Create {@link ByteBufferReadableStreamChannel} with random bytes.
   * 2. Calls the different read operations of {@link ByteBufferReadableStreamChannel} and checks that the data read
   * matches the data used to create the {@link ByteBufferReadableStreamChannel}.
   * @throws IOException
   */
  @Test
  public void commonCaseTest()
      throws IOException {
    readToChannelTest();
  }

  /**
   * Tests that the right exceptions are thrown when read operations fail.
   * @throws IOException
   */
  @Test
  public void readFailureTest()
      throws IOException {
    String errMsg = "@@ExpectedExceptionMessage@@";
    byte[] in = fillRandomBytes(new byte[1]);
    ByteBufferReadableStreamChannel byteBufferReadableStreamChannel =
        new ByteBufferReadableStreamChannel(ByteBuffer.wrap(in));

    try {
      byteBufferReadableStreamChannel.read(new BadWritableChannel(new IOException(errMsg)));
      fail("Should have failed because BadWritableChannel would have thrown exception");
    } catch (IOException e) {
      assertEquals("Exception message does not match expected", errMsg, e.getMessage());
    }

    byteBufferReadableStreamChannel.close();
    try {
      byteBufferReadableStreamChannel.read(new ByteBufferChannel(ByteBuffer.allocate(1)));
      fail("Should have failed because ByteBufferReadableStreamChannel should have thrown ClosedChannelException");
    } catch (ClosedChannelException e) {
      // expected. nothing to do.
    }
  }

  /**
   * Tests behavior of read operations on some corner cases.
   * <p/>
   * Corner case list:
   * 1. Blob size is 0.
   * @throws IOException
   */
  @Test
  public void readAndWriteCornerCasesTest()
      throws IOException {
    // 0 sized blob.
    ByteBufferReadableStreamChannel byteBufferReadableStreamChannel =
        new ByteBufferReadableStreamChannel(ByteBuffer.allocate(0));
    assertTrue("ByteBufferReadableStreamChannel is not open", byteBufferReadableStreamChannel.isOpen());
    assertEquals("There should have been no bytes to read", -1,
        byteBufferReadableStreamChannel.read(new ByteBufferChannel(ByteBuffer.allocate(0))));
  }

  /**
   * Tests that no exceptions are thrown on repeating idempotent operations. Does <b><i>not</i></b> currently test that
   * state changes are idempotent.
   * @throws IOException
   */
  @Test
  public void idempotentOperationsTest()
      throws IOException {
    byte[] in = fillRandomBytes(new byte[1]);
    ByteBufferReadableStreamChannel byteBufferReadableStreamChannel =
        new ByteBufferReadableStreamChannel(ByteBuffer.wrap(in));
    assertTrue("ByteBufferReadableStreamChannel is not open", byteBufferReadableStreamChannel.isOpen());
    byteBufferReadableStreamChannel.close();
    assertFalse("ByteBufferReadableStreamChannel is not closed", byteBufferReadableStreamChannel.isOpen());
    // should not throw exception.
    byteBufferReadableStreamChannel.close();
    assertFalse("ByteBufferReadableStreamChannel is not closed", byteBufferReadableStreamChannel.isOpen());
  }

  // helpers
  // general
  private byte[] fillRandomBytes(byte[] in) {
    new Random().nextBytes(in);
    return in;
  }

  // commonCaseTest() helpers
  private void readToChannelTest()
      throws IOException {
    byte[] in = fillRandomBytes(new byte[1024]);
    ByteBufferReadableStreamChannel byteBufferReadableStreamChannel =
        new ByteBufferReadableStreamChannel(ByteBuffer.wrap(in));
    assertTrue("ByteBufferReadableStreamChannel is not open", byteBufferReadableStreamChannel.isOpen());
    assertEquals("Size returned by ByteBufferReadableStreamChannel did not match source array size", in.length,
        byteBufferReadableStreamChannel.getSize());
    ByteBufferChannel channel =
        new ByteBufferChannel(ByteBuffer.allocate((int) byteBufferReadableStreamChannel.getSize()));
    // should be able to read all the data in one read
    int bytesWritten = byteBufferReadableStreamChannel.read(channel);
    assertEquals("Data size written did not match source byte array size", in.length, bytesWritten);
    assertArrayEquals("Source bytes and bytes in channel did not match", in, channel.getBuffer().array());
  }
}

/**
 * A {@link WritableByteChannel} that throws an {@link IOException} with a customizable message (provided at
 * construction time) on a call to {@link #write(ByteBuffer)}.
 */
class BadWritableChannel implements WritableByteChannel {
  private final AtomicBoolean channelOpen = new AtomicBoolean(true);
  private final IOException exceptionToThrow;

  public BadWritableChannel(IOException exceptionToThrow) {
    this.exceptionToThrow = exceptionToThrow;
  }

  @Override
  public int write(ByteBuffer src)
      throws IOException {
    throw exceptionToThrow;
  }

  @Override
  public boolean isOpen() {
    return channelOpen.get();
  }

  @Override
  public void close()
      throws IOException {
    channelOpen.set(false);
  }
}
