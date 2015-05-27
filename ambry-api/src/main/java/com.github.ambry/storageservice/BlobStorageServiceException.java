package com.github.ambry.storageservice;

/**
 * TODO: write description
 */
public class BlobStorageServiceException extends Exception {
  private final BlobStorageServiceErrorCode error;

  public BlobStorageServiceException(String message, BlobStorageServiceErrorCode error) {
    super(message);
    this.error = error;
  }

  public BlobStorageServiceException(String message, Throwable e, BlobStorageServiceErrorCode error) {
    super(message, e);
    this.error = error;
  }

  public BlobStorageServiceException(Throwable e, BlobStorageServiceErrorCode error) {
    super(e);
    this.error = error;
  }

  public BlobStorageServiceErrorCode getErrorCode() {
    return this.error;
  }
}
