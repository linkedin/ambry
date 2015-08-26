package com.github.ambry.router;

import com.github.ambry.network.ReadableStreamChannel;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Random;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;


/**
 * Tests functionality of {@link ReadableStreamChannelInputStream}.
 */
public class ReadableStreamChannelInputStreamTest {

  @Test
  public void commonCaseTest()
      throws IOException {
    byte[] in = new byte[1024];
    new Random().nextBytes(in);
    readByteByByteTest(in);
    readPartByPartTest(in);
    readAllAtOnceTest(in);
  }

  @Test
  public void readErrorCasesTest()
      throws IOException {
    byte[] in = new byte[1024];
    new Random().nextBytes(in);
    InputStream srcInputStream = new ByteArrayInputStream(in);
    ReadableStreamChannel channel = new DataStreamChannel(srcInputStream, in.length);
    InputStream dstInputStream = new ReadableStreamChannelInputStream(channel);
    try {
      dstInputStream.read(null, 0, in.length);
    } catch (NullPointerException e) {
      // expected. nothing to do.
    }

    byte[] out = new byte[in.length];
    try {
      dstInputStream.read(out, -1, out.length);
    } catch (IndexOutOfBoundsException e) {
      // expected. nothing to do.
    }

    try {
      dstInputStream.read(out, 0, -1);
    } catch (IndexOutOfBoundsException e) {
      // expected. nothing to do.
    }

    try {
      dstInputStream.read(out, 0, out.length + 1);
    } catch (IndexOutOfBoundsException e) {
      // expected. nothing to do.
    }

    assertEquals("Bytes read should have been 0 because passed len was 0", 0, dstInputStream.read(out, 0, 0));
  }

  // commonCaseTest() helpers
  private void readByteByByteTest(byte[] in)
      throws IOException {
    InputStream srcInputStream = new ByteArrayInputStream(in);
    ReadableStreamChannel channel = new DataStreamChannel(srcInputStream, in.length);
    InputStream dstInputStream = new ReadableStreamChannelInputStream(channel);
    for (int i = 0; i < in.length; i++) {
      assertEquals("Byte [" + i + "] does not match expected", in[i], (byte) dstInputStream.read());
    }
    assertEquals("Did not receive expected EOF", -1, dstInputStream.read());
  }

  private void readPartByPartTest(byte[] in)
      throws IOException {
    InputStream srcInputStream = new ByteArrayInputStream(in);
    ReadableStreamChannel channel = new DataStreamChannel(srcInputStream, in.length);
    InputStream dstInputStream = new ReadableStreamChannelInputStream(channel);
    byte[] out = new byte[in.length];
    for (int start = 0; start < in.length; ) {
      int end = Math.min(start + in.length / 4, in.length);
      int len = end - start;
      assertEquals("Bytes read did not match what was requested", len, dstInputStream.read(out, start, len));
      assertArrayEquals("Byte array obtained from InputStream did not match source", Arrays.copyOfRange(in, start, end),
          Arrays.copyOfRange(out, start, end));
      start = end;
    }
    assertEquals("Did not receive expected EOF", -1, dstInputStream.read(out, 0, out.length));
  }

  private void readAllAtOnceTest(byte[] in)
      throws IOException {
    InputStream srcInputStream = new ByteArrayInputStream(in);
    ReadableStreamChannel channel = new DataStreamChannel(srcInputStream, in.length);
    InputStream dstInputStream = new ReadableStreamChannelInputStream(channel);
    byte[] out = new byte[in.length];
    assertEquals("Bytes read did not match size of source array", in.length, dstInputStream.read(out));
    assertArrayEquals("Byte array obtained from InputStream did not match source", in, out);
    assertEquals("Did not receive expected EOF", -1, dstInputStream.read(out));
  }
}
