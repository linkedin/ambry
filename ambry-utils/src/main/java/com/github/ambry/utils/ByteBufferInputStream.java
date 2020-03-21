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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;


/**
 * A non-blocking {@link ByteBuffer} based {@link InputStream} extension that materializes the whole content in memory.
 */
public class ByteBufferInputStream extends InputStream {
  private ByteBuffer byteBuffer;
  private int mark;
  private int readLimit;

  public ByteBufferInputStream(ByteBuffer byteBuffer) {
    this.byteBuffer = byteBuffer;
    this.mark = -1;
    this.readLimit = -1;
  }

  /**
   * Reads 'size' amount of bytes from the stream into the buffer.
   * @param stream The stream from which bytes need to be read. If the underlying stream is SocketInputStream, it needs
   *               to be blocking.
   * @param size The size that needs to be read from the stream
   * @throws IOException
   */
  public ByteBufferInputStream(InputStream stream, int size) throws IOException {
    this.byteBuffer = Utils.getByteBufferFromInputStream(stream, size);
    this.mark = -1;
    this.readLimit = -1;
  }

  @Override
  public int read() throws IOException {
    if (!byteBuffer.hasRemaining()) {
      return -1;
    }
    return byteBuffer.get() & 0xFF;
  }

  @Override
  public int read(byte[] bytes, int offset, int length) throws IOException {
    if (bytes == null) {
      throw new NullPointerException();
    } else if (offset < 0 || length < 0 || length > bytes.length - offset) {
      throw new IndexOutOfBoundsException();
    } else if (length == 0) {
      return 0;
    }
    int count = Math.min(byteBuffer.remaining(), length);
    if (count == 0) {
      return -1;
    }
    byteBuffer.get(bytes, offset, count);
    return count;
  }

  @Override
  public int available() throws IOException {
    return byteBuffer.remaining();
  }

  @Override
  public synchronized void reset() throws IOException {
    if (readLimit == -1 || mark == -1) {
      throw new IOException("Mark not set before reset invoked.");
    }
    if (byteBuffer.position() - mark > readLimit) {
      throw new IOException("Read limit exceeded before reset invoked.");
    }
    byteBuffer.reset();
  }

  @Override
  public synchronized void mark(int readLimit) {
    this.mark = byteBuffer.position();
    this.readLimit = readLimit;
    byteBuffer.mark();
  }

  @Override
  public boolean markSupported() {
    return true;
  }

  public ByteBufferInputStream duplicate() {
    return new ByteBufferInputStream(byteBuffer.duplicate());
  }

  /**
   * Return the underlying read-only {@link ByteBuffer} associated with this ByteBufferInputStream.
   * <br>
   * Combining the reads from the returned {@link ByteBuffer} and the other read methods of this stream can lead to
   * unexpected behavior.
   * @return the underlying read-only {@link ByteBuffer} associated with this ByteBufferInputStream.
   */
  public ByteBuffer getByteBuffer() {
    return byteBuffer.asReadOnlyBuffer();
  }
}

