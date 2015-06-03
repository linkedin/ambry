package com.github.ambry.storageservice;

import java.util.concurrent.TimeUnit;


/**
 * TODO: write description
 */
public interface BlobStorageService {

  public void start() throws InstantiationException;

  public void shutdown() throws Exception;

  public String putBlob()
      throws BlobStorageServiceException;

  public Object getBlob()
      throws BlobStorageServiceException;

  public boolean deleteBlob()
      throws BlobStorageServiceException;

  /**
   * Execute a custom operation. The data must define the type of operation
   * @param data
   * @return
   * @throws BlobStorageServiceException
   */
  public ExecutionResult execute(ExecutionData data)
      throws BlobStorageServiceException;
}
