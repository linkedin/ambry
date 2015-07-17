package com.github.ambry.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;


/** A class that simulates an input stream of the given size that returns bytes of value zero
 *  Note that this is not safe for concurrent access.
 */

public class ZeroBytesInputStream extends InputStream {
  private long streamSize;
  private int position;

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
    return streamSize - position < Integer.MAX_VALUE ? (int) (streamSize - position) : Integer.MAX_VALUE;
  }
}

