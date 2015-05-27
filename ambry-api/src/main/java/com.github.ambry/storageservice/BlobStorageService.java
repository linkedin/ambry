package com.github.ambry.storageservice;

/**
 * TODO: write description
 */
public interface BlobStorageService {
  public String putBlob()
      throws BlobStorageServiceException;

  public Object getBlob()
      throws BlobStorageServiceException;

  public boolean deleteBlob()
      throws BlobStorageServiceException;

  public ExecutionResult execute(ExecutionData data)
      throws BlobStorageServiceException;
}
