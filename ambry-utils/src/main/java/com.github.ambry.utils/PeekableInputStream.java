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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * InputStream to provide a peek functionality over any inputstream.
 * The peek advances the input stream, but saves the data in peekbuffer.
 * So reads followed by peeks on same {@code PeekableInputStream} object first read from the buffer before reading from underlying inputstream.
 */
public class PeekableInputStream extends InputStream {
  private final InputStream inputStream;
  private LinkedList<Byte> peekBuffer;
  private Logger logger = LoggerFactory.getLogger(getClass());

  public PeekableInputStream(InputStream inputStream) {
    this.inputStream = inputStream;
    peekBuffer = new LinkedList<>();
  }

  @Override
  public int read(byte[] b) throws IOException {
    return this.read(b, 0, b.length);
  }

  /**
   * Reads the next {@code len} bytes from the inputstream into byte array starting at position {@code off}.
   * Note that the byte being read might have been peek-ed before. In that case read it from the peek buffer.
   * @return int value of the byte being read
   * @throws IOException if an exception occurs during read.
   */
  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int idx = off;
    int ctr = 0;
    while (ctr < len && !peekBuffer.isEmpty()) {
      b[idx] = peekBuffer.removeFirst();
      idx++;
      ctr++;
    }
    if (ctr == len) {
      return ctr;
    }
    int ret = this.inputStream.read(b, idx, len - ctr);
    if (ret == -1 && ctr > 0) {
      return ctr;
    }
    return ret;
  }

  /**
   * Reads the next byte from the inputstream. Note that the byte being read might have been peek-ed before.
   * In that case read it from the peek buffer.
   * @return int value of the byte being read
   * @throws IOException if an exception occurs during read.
   */
  @Override
  public int read() throws IOException {
    if (!peekBuffer.isEmpty()) {
      return peekBuffer.removeFirst();
    }
    return this.inputStream.read();
  }

  @Override
  public int available() throws IOException {
    int available = inputStream.available();
    logger.trace("remaining bytes {}", available);
    return available;
  }

  @Override
  public void close() throws IOException {
    inputStream.close();
  }

  /**
   * Read the next byte from the input stream, and also buffers it so that it is available for later read using this object.
   * This logically provides peek-like functionality on any input stream by making sure that the peek-ed bytes are available for read later on.
   * @return int value of the byte being read
   * @throws IOException if an exception occurs during read.
   */
  public int peekNextByte() throws IOException {
    int val = this.inputStream.read();
    peekBuffer.add((byte) val);
    return val;
  }

  /**
   * Read a short value from the input stream using the peek functionality.
   * @return short value read.
   * @throws IOException if an exception occurs during read.
   */
  public short peekShort() throws IOException {
    int ch1 = peekNextByte();
    int ch2 = peekNextByte();
    if ((ch1 | ch2) < 0) {
      throw new EOFException();
    }
    return (short) ((ch1 << 8) + (ch2 << 0));
  }
}
