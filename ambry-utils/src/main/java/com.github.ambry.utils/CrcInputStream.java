package com.github.ambry.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.IOException;

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
    crc.update((byte)(val&0xFF));
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
}
