package com.github.ambry.messageformat;

public class DeserializedBlobProperties {
  private short blobPropertiesVersion;
  private BlobProperties blobProperties;

  public DeserializedBlobProperties (short blobPropertiesVersion, BlobProperties blobProperties) {
    this.blobPropertiesVersion = blobPropertiesVersion;
    this.blobProperties = blobProperties;
  }

  public short getBlobPropertiesVersion() {
    return blobPropertiesVersion;
  }

  public BlobProperties getBlobProperties() {
    return blobProperties;
  }
}
