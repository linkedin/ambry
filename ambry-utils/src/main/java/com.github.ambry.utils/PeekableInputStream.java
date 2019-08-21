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
import java.util.LinkedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int idx = off;
    int ctr = 0;
    while(ctr < len && !peekBuffer.isEmpty()) {
      b[idx] = peekBuffer.removeFirst();
      idx++;
      ctr++;
    }
    if(ctr == len)
      return ctr;
    int ret = this.inputStream.read(b, idx, len-ctr);
    if(ret == -1 && ctr > 0) {
      return ctr;
    }
    return ret;
  }

  @Override
  public int read() throws IOException {
    if(!peekBuffer.isEmpty()) {
      return peekBuffer.removeFirst();
    }
    int val = this.inputStream.read();
    return val;
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

  public int peek() throws IOException {
    int val = this.inputStream.read();
    peekBuffer.add((byte)val);
    return val;
  }
}
