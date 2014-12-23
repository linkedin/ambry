package com.github.ambry.messageformat;

/**
 * A BlobInfo class that contains both the blob property and the usermetadata for the blob
 */
public class BlobInfo {

  private BlobProperties blobProperties;

  private byte[] userMetadata;

  public BlobInfo(BlobProperties blobProperties, byte[] userMetadata) {
    this.blobProperties = blobProperties;
    this.userMetadata = userMetadata;
  }

  public BlobProperties getBlobProperties() {
    return blobProperties;
  }

  public byte[] getUserMetadata() {
    return userMetadata;
  }
}
