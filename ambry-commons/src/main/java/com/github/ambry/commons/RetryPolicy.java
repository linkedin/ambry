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

/**
 * The {@link RetryPolicy} defines the specific number of  maximum attempts, wait time in milliseconds and maximum
 * time in milliseconds for timeout. Users can provide their own implementations of policy for different
 * needs. Two implementations: {@link RetryPolicies#exponentialPolicy} and {@link RetryPolicies#fixedBackoffPolicy}} are
 * provided for users.
 */
public interface RetryPolicy {
  /**
   * Provide the maximum attempts for Retries to decide retry or not.
   */
  int maxAttempts();

  /**
   * Provide the wait time in milliseconds for current attempt for Retries.
   * @param attempts The number of tries that have been attempted.
   */
  int waitTimeMs(int attempts);
}
