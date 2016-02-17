package com.github.ambry.messageformat;

import java.io.InputStream;


/**
 * Contains the blob output
 */
public class BlobOutput {
  private long size;
  private InputStream stream;

  /**
   * The blob output that helps to read a blob
   * @param size The size of the blob
   * @param stream The stream that contains the blob
   */
  public BlobOutput(long size, InputStream stream) {
    this.size = size;
    this.stream = stream;
  }

  public long getSize() {
    return size;
  }

  public InputStream getStream() {
    return stream;
  }

}
