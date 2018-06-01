/*
 * Copyright 2018 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.frontend;

import com.github.ambry.rest.RestRequest;
import org.slf4j.Logger;


/**
 * Tracks metrics and logs progress of operations that accept callbacks.
 */
class CallbackTracker {
  private long operationStartTime = 0;
  private long processingStartTime = 0;

  private final String requestUri;
  private final Logger logger;
  private final FrontendMetrics.AsyncOperationMetrics asyncOperationMetrics;

  /**
   * Create a CallbackTracker that tracks a particular operation.
   * @param restRequest the {@link RestRequest} for the operation.
   * @param asyncOperationMetrics the {@link FrontendMetrics.AsyncOperationMetrics}
   */
  CallbackTracker(RestRequest restRequest, Logger logger, FrontendMetrics.AsyncOperationMetrics asyncOperationMetrics) {
    this.logger = logger;
    this.asyncOperationMetrics = asyncOperationMetrics;
    requestUri = restRequest.getUri();
  }

  /**
   * Marks that the operation being tracked has started.
   */
  void markOperationStart() {
    logger.trace("{} started for {}", asyncOperationMetrics.operationType, requestUri);
    operationStartTime = System.currentTimeMillis();
  }

  /**
   * Marks that the operation being tracked has ended and callback processing has started.
   */
  void markOperationEnd() {
    logger.trace("{} finished for {}", asyncOperationMetrics.operationType, requestUri);
    processingStartTime = System.currentTimeMillis();
    long operationTime = processingStartTime - operationStartTime;
    asyncOperationMetrics.operationTimeInMs.update(operationTime);
  }

  /**
   * Marks that the  callback processing has ended.
   */
  void markCallbackProcessingEnd() {
    logger.trace("Callback for {} of {} finished", asyncOperationMetrics.operationType, requestUri);
    long processingTime = System.currentTimeMillis() - processingStartTime;
    asyncOperationMetrics.callbackProcessingTimeInMs.update(processingTime);
  }

  /**
   * Mark if an exception occurred within the callback.
   */
  void markCallbackProcessingError() {
    logger.trace("Exception thrown in callback for {} of {}", asyncOperationMetrics.operationType, requestUri);
    asyncOperationMetrics.callbackProcessingError.inc();
  }
}
