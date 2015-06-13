package com.github.ambry.admin;

import com.github.ambry.restservice.RestServiceException;
import org.json.JSONObject;


/**
 * Interface for all custom task executors
 *
 * // TODO: The utility of this interface is under review. Please comment.
 */
public interface TaskExecutor {
  /**
   * Execute the operation given the executionData.
   * @param data
   * @return
   * @throws RestServiceException
   */
  public JSONObject execute(AdminExecutionData data)
      throws RestServiceException;
}
