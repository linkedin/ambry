package com.github.ambry.router;

import com.github.ambry.utils.ByteBufferChannel;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.WritableByteChannel;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


/**
 * Tests functionality of {@link DataStreamChannel}.
 */
public class DataStreamChannelTest {

  /**
   * Tests the common case read operations i.e
   * 1. Create {@link DataStreamChannel} with random bytes.
   * 2. Calls the different read operations of {@link DataStreamChannel} and checks that the data read matches the data
   * used to create the {@link DataStreamChannel}.
   * @throws IOException
   */
  @Test
  public void commonCaseTest()
      throws IOException {
    readToChannelTest();
  }

  /**
   * Tests that the right exceptions are thrown when instantiation of {@link DataStreamChannel} fails.
   * @throws IOException
   */
  @Test
  public void instantiationFailureTests()
      throws IOException {
    byte[] in = new byte[1024];
    try {
      new DataStreamChannel(new ByteArrayInputStream(in), in.length + 1);
      fail("Should have failed because size input provided is greater than actual stream length");
    } catch (IllegalStateException e) {
      // expected. nothing to do.
    }

    String errMsg = "@@ExpectedExceptionMessage@@";
    try {
      new DataStreamChannel(new BadInputStream(new IOException(errMsg)), 1);
      fail("Should have failed because BadInputStream would have thrown exception");
    } catch (IOException e) {
      assertEquals("Exception message does not match expected", errMsg, e.getMessage());
    }
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
    DataStreamChannel dataStreamChannel = new DataStreamChannel(new ByteArrayInputStream(in), in.length);

    try {
      dataStreamChannel.read(new BadWritableChannel(new IOException(errMsg)));
      fail("Should have failed because BadWritableChannel would have thrown exception");
    } catch (IOException e) {
      assertEquals("Exception message does not match expected", errMsg, e.getMessage());
    }

    dataStreamChannel.close();
    try {
      dataStreamChannel.read(new ByteBufferChannel(ByteBuffer.allocate(1)));
      fail("Should have failed because DataStreamChannel should have thrown ClosedChannelException");
    } catch (ClosedChannelException e) {
      // expected. nothing to do.
    }
  }

  /**
   * Tests behaviour of read operations on some corner cases.
   * <p/>
   * Corner case list:
   * 1. Blob size is 0.
   * @throws IOException
   */
  @Test
  public void readAndWriteCornerCasesTest()
      throws IOException {
    // 0 sized blob.
    DataStreamChannel dataStreamChannel = new DataStreamChannel(new ByteArrayInputStream(new byte[0]), 0);
    assertEquals("There should have been no bytes to read", -1,
        dataStreamChannel.read(new ByteBufferChannel(ByteBuffer.allocate(0))));
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
    DataStreamChannel dataStreamChannel = new DataStreamChannel(new ByteArrayInputStream(in), in.length);
    dataStreamChannel.close();
    // should not throw exception.
    dataStreamChannel.close();
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
    byte[] in = new byte[1024];
    DataStreamChannel dataStreamChannel =
        new DataStreamChannel(new ByteArrayInputStream(fillRandomBytes(in)), in.length);
    assertEquals("Size returned by DataStreamChannel did not match source array size", in.length,
        dataStreamChannel.getSize());
    ByteBufferChannel channel = new ByteBufferChannel(ByteBuffer.allocate((int) dataStreamChannel.getSize()));
    // should be able to read all the data in one read
    int bytesWritten = dataStreamChannel.read(channel);
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

/**
 * A {@link InputStream} that throws an {@link IOException} with a customizable message (provided at construction time)
 * on a call to {@link #read()}.
 */
class BadInputStream extends InputStream {
  private final IOException exceptionToThrow;

  public BadInputStream(IOException exceptionToThrow) {
    this.exceptionToThrow = exceptionToThrow;
  }

  @Override
  public int read()
      throws IOException {
    throw exceptionToThrow;
  }
}
