package com.github.ambry.messageformat;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.NonReadableChannelException;
import java.nio.channels.WritableByteChannel;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;

import static org.junit.Assert.*;


/**
 * TODO: write description
 */
public class BlobOutputTest {

  @Test
  public void commonCaseTest()
      throws IOException {
    readTest();
    writeToTest();
  }

  @Test
  public void openForReadingFailureTests()
      throws IOException {
    byte[] in = new byte[1024];
    // try to reopen channel
    BlobOutput blobOutput = getRandomBlobOutputAsOpenedChannel(in);
    try {
      blobOutput.openForReading();
      fail("Tried to reopen BlobOutput for reading. Should have failed.");
    } catch (IllegalStateException e) {
      // expected. nothing to do.
    } finally {
      cleanUpBlobOutput(blobOutput);
    }

    // try to open channel after close
    blobOutput = new BlobOutput(in.length, new ByteArrayInputStream(in));
    blobOutput.close();
    try {
      blobOutput.openForReading();
      fail("Tried to open closed BlobOutput. Should have failed");
    } catch (ClosedChannelException e) {
      // expected. nothing to do.
    } finally {
      cleanUpBlobOutput(blobOutput);
    }

    // read lesser than stream size data
    blobOutput = new BlobOutput(in.length + 1, new ByteArrayInputStream(in));
    try {
      blobOutput.openForReading();
      fail("Should have failed because data loaded into buffer was lesser than actual data in stream");
    } catch (IOException e) {
      // expected. nothing to do.
    } finally {
      cleanUpBlobOutput(blobOutput);
    }

    // read more than stream size data
    blobOutput = new BlobOutput(in.length - 1, new ByteArrayInputStream(in));
    try {
      blobOutput.openForReading();
      fail("Should have failed because there was still data remaining after buffer was loaded with blob size bytes");
    } catch (IOException e) {
      // expected. nothing to do.
    } finally {
      cleanUpBlobOutput(blobOutput);
    }
  }

  @Test
  public void readAndWriteToFailureTest()
      throws IOException {
    byte[] in = new byte[1];
    fillRandomBytes(in);
    ByteChannel channel = new ByteChannel(0);
    InputStream data = new ByteArrayInputStream(in);
    BlobOutput blobOutput = new BlobOutput(in.length, data);
    ByteBuffer buffer = ByteBuffer.allocate(0);
    try {
      blobOutput.read(buffer);
      fail("Tried to read from un-opened BlobOutput. Should have failed");
    } catch (NonReadableChannelException e) {
      // expected. nothing to do.
    }

    try {
      blobOutput.writeTo(channel);
      fail("Tried writeTo from un-opened BlobOutput. Should have failed");
    } catch (NonReadableChannelException e) {
      // expected. nothing to do.
    }

    blobOutput.openForReading();
    blobOutput.close();
    try {
      blobOutput.read(buffer);
      fail("Tried to read from closed BlobOutput. Should have failed");
    } catch (ClosedChannelException e) {
      // expected. nothing to do.
    }

    try {
      blobOutput.writeTo(channel);
      fail("Tried writeTo from closed BlobOutput. Should have failed");
    } catch (ClosedChannelException e) {
      // expected. nothing to do.
    }
  }

  @Test
  public void readAndWriteCornerCasesTest()
      throws IOException {
    // 0 sized blob.
    ByteChannel channel = new ByteChannel(0);
    BlobOutput blobOutput = getRandomBlobOutputAsOpenedChannel(new byte[0]);
    ByteBuffer buffer = ByteBuffer.allocate(0);

    assertEquals("There should have been no bytes to read", -1, blobOutput.read(buffer));
    assertEquals("There should have been no bytes to write", -1, blobOutput.writeTo(channel));
  }

  // helpers
  // general
  private void fillRandomBytes(byte[] in) {
    Random random = new Random();
    random.nextBytes(in);
  }

  private void cleanUpBlobOutput(BlobOutput blobOutput)
      throws IOException {
    blobOutput.close();
    assertFalse("BlobOutput channel is not closed", blobOutput.isOpen());
  }

  // commonCaseTest() helpers
  private void readTest()
      throws IOException {
    byte[] in = new byte[1024];
    BlobOutput blobOutput = getRandomBlobOutputAsOpenedChannel(in);
    byte[] out = new byte[in.length];
    ByteBuffer dst = ByteBuffer.wrap(out);
    // should be able to load all data in one read
    int bytesRead = blobOutput.read(dst);
    assertEquals("Data size read did not match source byte array size", in.length, bytesRead);
    assertArrayEquals("Source and received byte arrays did not match", in, out);
    cleanUpBlobOutput(blobOutput);
  }

  private void writeToTest()
      throws IOException {
    byte[] in = new byte[1024];
    BlobOutput blobOutput = getRandomBlobOutputAsOpenedChannel(in);
    ByteChannel channel = new ByteChannel(in.length);
    // should be able to write all data in one writeTo
    int bytesWritten = blobOutput.writeTo(channel);
    assertEquals("Data size written did not match source byte array size", in.length, bytesWritten);
    byte[] out = channel.getBuf();
    assertArrayEquals("Source and received byte arrays did not match", in, out);
    cleanUpBlobOutput(blobOutput);
  }

  private BlobOutput getRandomBlobOutputAsOpenedChannel(byte[] in)
      throws IOException {
    fillRandomBytes(in);
    InputStream data = new ByteArrayInputStream(in);
    BlobOutput blobOutput = new BlobOutput(in.length, data).openForReading();
    assertTrue("BlobOutput channel is not open for reading", blobOutput.isOpen());
    return blobOutput;
  }
}

class ByteChannel implements WritableByteChannel {
  private final AtomicBoolean channelOpen = new AtomicBoolean(true);
  private final byte[] buf;

  byte[] getBuf() {
    return buf;
  }

  public ByteChannel(int capacity) {
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
