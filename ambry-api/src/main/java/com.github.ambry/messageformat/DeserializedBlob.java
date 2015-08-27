package com.github.ambry.messageformat;

public class DeserializedBlob {
  private short version;
  private BlobOutput blobOutput;

  public DeserializedBlob(short blobVersion, BlobOutput blobOutput) {
    this.version = blobVersion;
    this.blobOutput = blobOutput;
  }

  public short getVersion() {
    return version;
  }

  public BlobOutput getBlobOutput() {
    return blobOutput;
  }
}
