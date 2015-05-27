package com.github.ambry.admin;

import com.github.ambry.storageservice.BlobStorageServiceException;


/**
 * TODO: write description
 */
public interface TaskExecutor {
  public AdminExecutionResult execute(AdminExecutionData data)
      throws BlobStorageServiceException;
}
