package com.github.ambry.utils;

import java.io.OutputStream;
import java.io.IOException;


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
  public void write(int b)
      throws IOException {
    stream.write(b);
    crc.update((byte) (b & 0xFF));
  }

  @Override
  public void write(byte b[])
      throws IOException {
    stream.write(b);
    crc.update(b, 0, b.length);
  }

  @Override
  public void write(byte b[], int off, int len)
      throws IOException {
    stream.write(b, off, len);
    crc.update(b, off, len);
  }

  @Override
  public void close()
      throws IOException {
    stream.close();
  }

  public long getValue() {
    return crc.getValue();
  }
}


