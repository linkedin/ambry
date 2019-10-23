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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An inputstream that calculates Crc on the fly
 */
public class CrcInputStream extends InputStream {
  private Crc32 crc;
  private InputStream stream;
  private Logger logger = LoggerFactory.getLogger(getClass());

  /**
   * Create a CrcInputStream using the specified CRC generator
   * @param in
   */
  public CrcInputStream(InputStream in) {
    this(new Crc32(), in);
  }

  public CrcInputStream(Crc32 crc, InputStream in) {
    this.crc = crc;
    this.stream = in;
  }

  @Override
  public int read() throws IOException {
    int val = stream.read();
    crc.update((byte) (val & 0xFF));
    return val;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return this.read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int ret = stream.read(b, off, len);
    crc.update(b, off, ret);
    return ret;
  }

  /**
   * update crc with the all the data reading from given buffer.
   * @param buffer The buffer to update the crc value.
   * @throws IOException any I/O error.
   */
  public void updateCrc(ByteBuffer buffer) throws IOException {
    crc.update(buffer);
  }

  @Override
  public int available() throws IOException {
    int available = stream.available();
    logger.trace("remaining bytes {}", available);
    return available;
  }

  @Override
  public void close() throws IOException {
    stream.close();
  }

  public long getValue() {
    return crc.getValue();
  }

  /**
   * Return underlying {@link InputStream}
   * @return Underlying {@link InputStream}
   */
  public InputStream getUnderlyingInputStream() {
    return stream;
  }
}
