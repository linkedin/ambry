package com.github.ambry.cloud;

import java.sql.Blob;


public class BlobEvent {
  private String blobId;
  private BlobOperation blobOperation;
  // need TTL?

  public BlobEvent(String blobId, BlobOperation blobOperation) {
    this.blobId = blobId;
    this.blobOperation = blobOperation;
  }

  public String getBlobId() {
    return blobId;
  }

  public void setBlobId(String blobId) {
    this.blobId = blobId;
  }

  public BlobOperation getBlobOperation() {
    return blobOperation;
  }

  public void setBlobOperation(BlobOperation blobOperation) {
    this.blobOperation = blobOperation;
  }

}
