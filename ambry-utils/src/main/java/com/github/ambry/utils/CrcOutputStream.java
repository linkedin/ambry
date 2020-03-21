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
import java.io.OutputStream;


/**
 * An outputstream that calculates Crc on the fly
 */
public class CrcOutputStream extends OutputStream {
  private Crc32 crc;
  private OutputStream stream;

  /**
   * Create a CrcOutputStream using the specified CRC generator
   * @param out
   */
  public CrcOutputStream(OutputStream out) {
    this(new Crc32(), out);
  }

  public CrcOutputStream(Crc32 crc, OutputStream out) {
    this.crc = crc;
    this.stream = out;
  }

  @Override
  public void write(int b) throws IOException {
    stream.write(b);
    crc.update((byte) (b & 0xFF));
  }

  @Override
  public void write(byte b[]) throws IOException {
    stream.write(b);
    crc.update(b, 0, b.length);
  }

  @Override
  public void write(byte b[], int off, int len) throws IOException {
    stream.write(b, off, len);
    crc.update(b, off, len);
  }

  @Override
  public void close() throws IOException {
    stream.close();
  }

  public long getValue() {
    return crc.getValue();
  }
}


