package com.github.ambry.messageformat;

import java.io.InputStream;


/**
 * Contains the blob output along with the blob type
 */
public class BlobOutputInfo {
  private BlobType blobType;
  private long size;
  private InputStream stream;

  /**
   * The blob output info that helps to read the blob/metadata blob
   * @param blobType {@BlobType} of the blob
   * @param size The size of the blob
   * @param stream The stream that contains the blob
   */
  public BlobOutputInfo(BlobType blobType, long size, InputStream stream) {
    this.blobType = blobType;
    this.size = size;
    this.stream = stream;
  }

  public BlobType getBlobType(){
    return this.blobType;
  }

  public long getSize() {
    return size;
  }

  public InputStream getStream() {
    return stream;
  }

  public BlobOutput getBlobOutput(){
    return new BlobOutput(size, stream);
  }
}
