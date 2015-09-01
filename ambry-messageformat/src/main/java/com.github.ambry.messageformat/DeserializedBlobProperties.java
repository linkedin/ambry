package com.github.ambry.messageformat;

class DeserializedBlobProperties {
  private short version;
  private BlobProperties blobProperties;

  public DeserializedBlobProperties(short version, BlobProperties blobProperties) {
    this.version = version;
    this.blobProperties = blobProperties;
  }

  public short getVersion() {
    return version;
  }

  public BlobProperties getBlobProperties() {
    return blobProperties;
  }
}
