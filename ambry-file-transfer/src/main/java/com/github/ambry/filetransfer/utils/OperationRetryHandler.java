/**
 * Copyright 2025 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.filetransfer.utils;

import com.github.ambry.config.FileCopyBasedReplicationConfig;
import com.github.ambry.filetransfer.handler.FileCopyHandlerConfig;
import com.github.ambry.network.ConnectionPoolTimeoutException;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A class to handle retries for operations that can be retried.
 */
public class OperationRetryHandler {
  /**
   * The maximum number of retries for the API call
   */
  private final int maxRetries;

  /**
   * The backoff time in milliseconds between retries
   */
  private final int retryDelayInMS;

  private static final Logger logger = LoggerFactory.getLogger(OperationRetryHandler.class);

  /**
   * Constructor to create OperationRetryHandler
   * @param config the configuration of type {@link FileCopyHandlerConfig}
   */
  public OperationRetryHandler(@Nonnull FileCopyBasedReplicationConfig config) {
    Objects.requireNonNull(config, "config cannot be null");

    maxRetries = config.fileCopyHandlerMaxApiRetries;
    retryDelayInMS = config.fileCopyHandlerRetryBackoffMs;
  }

  /**
   * Execute the operation with retries
   * @param operation the operation to execute of type {@link RetryableOperation<T>}
   * @param operationName the name of the operation. Needed for logging.
   * @return the result of the operation of type T
   * @param <T> the return type of the executable operation
   * @throws IOException
   * @throws ConnectionPoolTimeoutException
   * @throws InterruptedException
   */
  public <T> T executeWithRetry(@Nonnull RetryableOperation<T> operation,
      @Nonnull String operationName) throws Exception {
    Objects.requireNonNull(operation, "operation cannot be null");
    int attempts = 0;
    T result;

    while (true) {
      try {
        result = operation.execute();
        logger.info("{} succeeded after {} attempts", operationName, attempts);
        return result;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt(); // Preserve the interrupt status
        logMessageAndThrow(operationName, attempts, maxRetries, e);
      } catch (Exception e) {
        attempts++;
        logMessageAndThrow(operationName, attempts, maxRetries, e);
      }

      try {
        TimeUnit.MILLISECONDS.sleep(retryDelayInMS);
      } catch (InterruptedException sleepInterrupted) {
        Thread.currentThread().interrupt();
        logger.error("{} was interrupted during retry delay", operationName, sleepInterrupted);
        throw sleepInterrupted;
      }
    }
  }

  /**
   * Log the message and throw the exception
   * @param operationName the name of the operation
   * @param attempts the number of attempts
   * @param maxRetries the maximum number of retries
   * @param e the exception to throw
   */
  private void logMessageAndThrow(String operationName, int attempts, int maxRetries, Exception e) throws Exception {
    logger.warn("{} failed with an exception. Attempt {}/{}", operationName, attempts, maxRetries, e);
    if (attempts >= maxRetries) {
      logger.error("{} failed due to an exception after {} attempts", operationName, attempts, e);
      throw e;
    }
  }

  /**
   * Interface for operations that can be retried
   * @param <T> the return type of the operation
   */
  public interface RetryableOperation<T> {
    T execute() throws Exception;
  }
}
