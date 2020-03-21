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
package com.github.ambry.utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;


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
