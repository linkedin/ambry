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
import java.util.Arrays;


/** A class that simulates an input stream of the given size that returns bytes of value zero
 *  Note that this is not safe for concurrent access.
 */

public class ZeroBytesInputStream extends InputStream {
  private long streamSize;
  private long position;

  /**
   * Returns 'size' number of zero bytes when read from.
   * @param size The number of zero bytes to be returned.
   * @throws IOException
   */
  public ZeroBytesInputStream(long size)
      throws IOException {
    this.streamSize = size;
    this.position = 0;
  }

  @Override
  public int read()
      throws IOException {
    if (position < streamSize) {
      ++position;
      return 0;
    } else {
      return -1;
    }
  }

  @Override
  public int read(byte[] bytes, int offset, int length)
      throws IOException {
    int count = (int) Math.min(length, streamSize - position);
    Arrays.fill(bytes, offset, offset + count, (byte) 0);
    position += count;
    return count;
  }

  @Override
  public int available()
      throws IOException {
    /* available just has to be an estimate of the bytes remaining. So if the available bytes are greater than
       what an int can hold, return Integer.MAX_VALUE. */
    return streamSize - position < Integer.MAX_VALUE ? (int) (streamSize - position) : Integer.MAX_VALUE;
  }
}

