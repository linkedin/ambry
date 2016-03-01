package com.github.ambry.messageformat;

import java.io.InputStream;


/**
 * Contains the blob stream along with some required info
 */
public class BlobData {
  private final BlobType blobType;
  private final long size;
  private final InputStream stream;

  /**
   * The blob data contains the stream and other required info
   * @param blobType {@link BlobType} of the blob
   * @param size The size of the blob
   * @param stream The stream that contains the blob
   */
  public BlobData(BlobType blobType, long size, InputStream stream) {
    this.blobType = blobType;
    this.size = size;
    this.stream = stream;
  }

  public BlobType getBlobType() {
    return this.blobType;
  }

  public long getSize() {
    return size;
  }

  public InputStream getStream() {
    return stream;
  }
}
