package com.github.ambry.commons;

import com.github.ambry.messageformat.BlobInfo;
import com.github.ambry.messageformat.CompositeBlobInfo;

public class MetadataChunk {

  private BlobInfo blobInfo;
  private CompositeBlobInfo compositeBlobInfo;

  public MetadataChunk(CompositeBlobInfo compositeBlobInfo) {
    this.blobInfo = null;
    this.compositeBlobInfo = compositeBlobInfo;
  }

  public MetadataChunk(BlobInfo blobInfo, CompositeBlobInfo compositeBlobInfo) {
    this.blobInfo = blobInfo;
    this.compositeBlobInfo = compositeBlobInfo;
  }
  
  public BlobInfo getBlobInfo() {
    return blobInfo;
  }

  public CompositeBlobInfo getCompositeBlobInfo() {
    return compositeBlobInfo;
  }

}
