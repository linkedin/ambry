package com.github.ambry.admin;

import com.github.ambry.restservice.RestServiceException;


/**
 * TODO: Under review
 */
public interface TaskExecutor {
  public AdminExecutionResult execute(AdminExecutionData data)
      throws RestServiceException;
}
