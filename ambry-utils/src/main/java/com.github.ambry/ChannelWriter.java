package com.github.ambry;

import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: srsubram
 * Date: 10/15/13
 * Time: 4:50 PM
 * To change this template use File | Settings | File Templates.
 */
public class ChannelWriter {
  private final WritableByteChannel channel;
  private static final int size = 1024;
  private final ByteBuffer buffer = ByteBuffer.allocate(size);

  public ChannelWriter(WritableByteChannel channel) {
    this.channel = channel;
  }

  public void writeInt(int value) throws IOException {
    buffer.clear();
    buffer.putInt(value);
    buffer.flip();
    channel.write(buffer);
  }

  public void writeLong(long value) throws IOException {
    buffer.clear();
    buffer.putLong(value);
    buffer.flip();
    channel.write(buffer);
  }

  public void writeShort(short value) throws IOException {
    buffer.clear();
    buffer.putShort(value);
    buffer.flip();
    channel.write(buffer);
  }

  public void writeString(String s) throws IOException {
    InputStream stream = new ByteArrayInputStream(s.getBytes("UTF-8"));
    writeStream(stream, s.length());
  }

  public void writeStream(InputStream stream, long streamSize) throws IOException {
    buffer.clear();
    writeLong(streamSize);
    buffer.clear();
    for (int i = 0; i < streamSize; i++) {
      buffer.put((byte) stream.read());
      if (i % size == 0) {
        buffer.flip();
        channel.write(buffer);
        buffer.clear();
      }
    }
    buffer.flip();
    channel.write(buffer);
  }

  public void writeBuffer(ByteBuffer buffer) throws IOException {
    writeLong(buffer.limit());
    channel.write(buffer);
  }
}
