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
package com.github.ambry.filetransfer;

import com.github.ambry.network.ConnectionPoolTimeoutException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OperationRetryHandler {
  // TODO: From cfg2
  private static final int MAX_RETRIES = 3;
  private static final int RETRY_DELAY_MS = 500;

  private static final Logger logger = LoggerFactory.getLogger(OperationRetryHandler.class);

  public static <T> T executeWithRetry(RetryableOperation<T> operation, String operationName)
      throws IOException, ConnectionPoolTimeoutException, InterruptedException {
    int attempts = 0;
    while (true) {
      try {
        return operation.execute();
      } catch (IOException | ConnectionPoolTimeoutException | InterruptedException e) {
        attempts++;
        if (attempts >= MAX_RETRIES) {
          logger.error("{} failed after {} attempts", operationName, attempts, e);
          throw e;
        }
        logger.info("{} failed. Retrying {}/{}", operationName, attempts, MAX_RETRIES);

        // TODO: Communicate with FCThread about the retry/sleep
        TimeUnit.MILLISECONDS.sleep(RETRY_DELAY_MS);
      }
    }
  }

  public interface RetryableOperation<T> {
    T execute() throws IOException, ConnectionPoolTimeoutException, InterruptedException;
  }
}


