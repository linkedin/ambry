package com.github.ambry.messageformat;

class DeserializedBlob {
  private short version;
  private BlobOutput blobOutput;

  public DeserializedBlob(short version, BlobOutput blobOutput) {
    this.version = version;
    this.blobOutput = blobOutput;
  }

  public short getVersion() {
    return version;
  }

  public BlobOutput getBlobOutput() {
    return blobOutput;
  }
}
