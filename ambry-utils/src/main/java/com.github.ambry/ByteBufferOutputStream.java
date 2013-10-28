package com.github.ambry;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Created with IntelliJ IDEA.
 * User: srsubram
 * Date: 10/15/13
 * Time: 6:49 PM
 * To change this template use File | Settings | File Templates.
 */
public class ByteBufferOutputStream extends OutputStream {
  private ByteBuffer buffer;

  public ByteBufferOutputStream(ByteBuffer buffer) {
    this.buffer = buffer;
  }

  public void write(int b) throws IOException {
    buffer.put((byte) b);
  }

  public void write(byte[] bytes, int off, int len) throws IOException {
    buffer.put(bytes, off, len);
  }
}
