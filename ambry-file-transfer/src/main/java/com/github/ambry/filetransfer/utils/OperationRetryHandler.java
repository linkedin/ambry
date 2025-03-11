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

import com.github.ambry.filetransfer.handler.FileCopyHandlerConfig;
import com.github.ambry.network.ConnectionPoolTimeoutException;
import com.github.ambry.protocol.RequestOrResponse;
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
  public OperationRetryHandler(@Nonnull FileCopyHandlerConfig config) {
    Objects.requireNonNull(config, "config cannot be null");

    maxRetries = config.fileCopyHandlerMaxApiRetries;
    retryDelayInMS = config.fileCopyHandlerRetryBackoffMs;
  }

  /**
   * Execute the operation with retries
   * @param operation the operation to execute of type {@link RetryableOperation<T>} where T is a subclass of {@link RequestOrResponse}
   * @param operationName the name of the operation. Needed for logging.
   * @return the result of the operation of type T where T is a subclass of {@link RequestOrResponse}
   * @param <T>
   * @throws IOException
   * @throws ConnectionPoolTimeoutException
   * @throws InterruptedException
   */
  public <T extends RequestOrResponse> T executeWithRetry(@Nonnull RetryableOperation<T> operation, @Nonnull String operationName)
      throws IOException, ConnectionPoolTimeoutException, InterruptedException {
    Objects.requireNonNull(operation, "operation cannot be null");

    int attempts = 0;
    while (true) {
      try {
        return operation.execute();
      } catch (IOException | ConnectionPoolTimeoutException | InterruptedException e) {
        attempts++;
        if (attempts >= maxRetries) {
          logger.error("{} failed after {} attempts", operationName, attempts, e);
          throw e;
        }
        logger.info("{} failed. Retrying {}/{}", operationName, attempts, maxRetries, e);

        // TODO: Communicate with FCThread about the retry/sleep
        TimeUnit.MILLISECONDS.sleep(retryDelayInMS);
      }
    }
  }

  /**
   * Interface for operations that can be retried
   * @param <T> the type of the operation where T is a subclass of {@link RequestOrResponse}
   */
  public interface RetryableOperation<T extends RequestOrResponse> {
    T execute() throws IOException, ConnectionPoolTimeoutException, InterruptedException;
  }
}
