package com.github.ambry.server;

import com.github.ambry.messageformat.BlobProperties;


/**
 * All of the content of a blob
 */
class Payload {
  public byte[] blob;
  public byte[] metadata;
  public BlobProperties blobProperties;
  public String blobId;

  public Payload(BlobProperties blobProperties, byte[] metadata, byte[] blob, String blobId) {
    this.blobProperties = blobProperties;
    this.metadata = metadata;
    this.blob = blob;
    this.blobId = blobId;
  }
}

