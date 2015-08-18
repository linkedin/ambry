package com.github.ambry.messageformat;

import com.github.ambry.network.ReadableStreamChannel;


/**
 * Contains blob data in the form of a {@link ReadableStreamChannel} along with {@link BlobProperties}.
 */
public class Blob {
  public final BlobProperties blobProperties;
  public final ReadableStreamChannel blobData;

  public BlobProperties getBlobProperties() {
    return blobProperties;
  }

  public ReadableStreamChannel getBlobData() {
    return blobData;
  }

  public Blob(BlobProperties blobProperties, ReadableStreamChannel blobData) {
    this.blobProperties = blobProperties;
    this.blobData = blobData;
  }
}
