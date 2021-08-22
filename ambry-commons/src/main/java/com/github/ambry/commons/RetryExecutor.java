/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.commons;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Retries is a library that can be used to execute retry operations or functions with user-specified policy.
 * A {@link Consumer} call with a {@link Callback} is needed to provide the execution logic for retry,
 * when execution may not be successful with exception. You can provide a user-{@link Callback},
 * which will be executed after all policy defined retry executions are done successfully or not.
 */
public class RetryExecutor {
  private final ScheduledExecutorService scheduler;
  private static final Logger logger = LoggerFactory.getLogger(RetryExecutor.class);

  /**
   * Constructor for {@link RetryExecutor}.
   * @param scheduler The executor of function call with specific wait time provided by policy.
   */
  public RetryExecutor(ScheduledExecutorService scheduler) {
    this.scheduler = scheduler;
  }

  /**
   * Starts retriable function call with specific {@link RetryPolicy} and callback function.
   * @param policy The {@link RetryPolicy} to schedule retry.
   * @param call The function call that will be executed and retried with failure.
   * @param isRetriable user defined {@link Predicate} for retry or not when exception happen.
   * @param userCallback The user defined {@link Callback} to be executed after success or if all retry attempts failed.
   */
  public <T> void runWithRetries(RetryPolicy policy, Consumer<Callback<T>> call, Predicate<Throwable> isRetriable, Callback<T> userCallback) {
    recursiveAsyncRetry(call, isRetriable, policy, userCallback, 0);
  }

  /**
   * Recursively retrying on the task call with specific policy and user defined callback.
   * @param call The function call will be executed and retried with failure.
   * @param isRetriable user defined {@link Predicate} for retry or not when exception happen.
   * @param policy The {@link RetryPolicy} to schedule retrying.
   * @param userCallback The user defined {@link Callback} to be executed after all retry attempts failed.
   * @param attempts The number of retries has been attempted.
   */
  private <T> void recursiveAsyncRetry(Consumer<Callback<T>> call, Predicate<Throwable> isRetriable, RetryPolicy policy, Callback<T> userCallback,
      int attempts) {
    call.accept((result, exception) -> {
      if (exception != null) {
        int currAttempts = attempts + 1;
        if (currAttempts < policy.maxAttempts() && isRetriable.test(exception)) {
          int waitTimeMs = policy.waitTimeMs(currAttempts);
          logger.info("{} of {} attempts failed, will keep retrying after a {} ms backoff. exception='{}'",
              currAttempts, policy.maxAttempts(), waitTimeMs, exception);
          //Don't need the scheduler if no need to wait for retry and show warn message if scheduler is null but wait time larger than 0;
          if (scheduler == null) {
            if (waitTimeMs > 0) {
              logger.warn("No scheduler for delay execution. waitTimeMs: {}", waitTimeMs);
            }
            recursiveAsyncRetry(call, isRetriable, policy, userCallback, currAttempts);
          } else {
            scheduler.schedule(() -> recursiveAsyncRetry(call, isRetriable, policy, userCallback, currAttempts),
                waitTimeMs, TimeUnit.MILLISECONDS);
          }
        } else {
          logger.info("{} of {} attempts failed, completing operation. See exception below:", currAttempts,
              policy.maxAttempts(), exception);
          userCallback.onCompletion(null, exception);
        }
      } else {
        userCallback.onCompletion(result, null);
      }
    });
  }
}
