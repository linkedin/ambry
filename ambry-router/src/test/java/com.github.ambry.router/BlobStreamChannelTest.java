package com.github.ambry.router;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;


/**
 * Tests functionality of {@link BlobStreamChannel}.
 */
public class BlobStreamChannelTest {

  /**
   * Tests the common case read operations i.e
   * 1. Create {@link BlobStreamChannel} with random bytes.
   * 2. Calls the different read operations of {@link BlobStreamChannel} and checks that the data read matches the data
   * used to create the {@link BlobStreamChannel}.
   * @throws IOException
   */
  @Test
  public void commonCaseTest()
      throws IOException {
    readToChannelTest();
  }

  /**
   * Tests that the right exceptions are thrown when instantiation of {@link BlobStreamChannel} fails.
   * @throws IOException
   */
  @Test
  public void instantiationFailureTests()
      throws IOException {
    byte[] in = new byte[1024];
    try {
      new BlobStreamChannel(new ByteArrayInputStream(in), in.length + 1);
      fail("Should have failed because size input provided is greater than actual stream length");
    } catch (IllegalStateException e) {
      // expected. nothing to do.
    }

    String errMsg = "@@ExpectedExceptionMessage@@";
    try {
      new BlobStreamChannel(new BadInputStream(new IOException(errMsg)), 1);
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
    BlobStreamChannel blobStreamChannel = new BlobStreamChannel(new ByteArrayInputStream(in), in.length);

    try {
      blobStreamChannel.read(new BadWritableChannel(new IOException(errMsg)));
      fail("Should have failed because BadWritableChannel would have thrown exception");
    } catch (IOException e) {
      assertEquals("Exception message does not match expected", errMsg, e.getMessage());
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
    BlobStreamChannel blobStreamChannel = new BlobStreamChannel(new ByteArrayInputStream(new byte[0]), 0);
    assertEquals("There should have been no bytes to read", -1, blobStreamChannel.read(new ByteArrayChannel(0)));
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
    BlobStreamChannel blobStreamChannel =
        new BlobStreamChannel(new ByteArrayInputStream(fillRandomBytes(in)), in.length);
    ByteArrayChannel channel = new ByteArrayChannel(in.length);
    // should be able to read all the data in one read
    int bytesWritten = blobStreamChannel.read(channel);
    assertEquals("Data size written did not match source byte array size", in.length, bytesWritten);
    assertArrayEquals("Source bytes and bytes in channel did not match", in, channel.getBuf());
  }
}

/**
 * A {@link WritableByteChannel} that receives bytes and stores them in a byte array. The byte array can be retrieved
 * later to check/consume bytes written to the channel.
 */
class ByteArrayChannel implements WritableByteChannel {
  private final AtomicBoolean channelOpen = new AtomicBoolean(true);
  private final byte[] buf;

  byte[] getBuf() {
    return buf;
  }

  /**
   * Creates a ByteArrayChannel with a fixed capacity that cannot be exceeded.
   * @param capacity the capacity of the backing byte array.
   */
  public ByteArrayChannel(int capacity) {
    buf = new byte[capacity];
  }

  @Override
  public int write(ByteBuffer src)
      throws IOException {
    int prevPosition = src.position();
    src.get(buf);
    return src.position() - prevPosition;
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
