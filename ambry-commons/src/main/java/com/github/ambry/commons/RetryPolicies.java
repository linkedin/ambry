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

import java.util.Objects;


/**
 * Various common {@link RetryPolicy} implementations that can be used by customers.
 */
public class RetryPolicies {
  private static final RetryPolicy DEFAULT_POLICY = exponentialPolicy(3, 100);

  private RetryPolicies() {
  }

  /**
   * @return a retry policy with sane defaults: exponential policy with 4 attempts and an initial backoff time of 100 ms
   */
  public static RetryPolicy defaultPolicy() {
    return DEFAULT_POLICY;
  }

  /**
   * @param maxAttempts The maximum attempts.
   * @param initialWaitTimeMs The initial wait time in milliseconds, wait time will grow exponentially with number of
   *                          attempts.
   * @return an exponential retry policy with the provided parameters.
   */
  public static RetryPolicy exponentialPolicy(int maxAttempts, int initialWaitTimeMs) {
    return new ExponentialPolicy(maxAttempts, initialWaitTimeMs);
  }

  /**
   * @param maxAttempts The maximum attempts.
   * @param waitTimeMs The wait time in milliseconds, wait time will not change with time.
   * @return a fixed-backoff retry policy with the provided parameters.
   */
  public static RetryPolicy fixedBackoffPolicy(int maxAttempts, int waitTimeMs) {
    return new FixedBackoffPolicy(maxAttempts, waitTimeMs);
  }

  /**
   * @param attempts  the number of attempts provided as an argument to {@link RetryPolicy#waitTimeMs}.
   * @param maxAttempts the maximum number of attempts for the policy
   * @throws IllegalArgumentException if {@code attempts} is not in the range {@code [1, maxAttempts)}
   */
  private static void checkWaitTimeMsArgs(int attempts, int maxAttempts) {
    if (attempts < 1 || attempts >= maxAttempts) {
      throw new IllegalArgumentException(
          "Number of attempts (" + attempts + ") must be in the range [1," + maxAttempts + ")");
    }
  }

  /**
   * The example {@link RetryPolicy} with wait time that grows exponentially with number of attempts.
   */
  private static class ExponentialPolicy implements RetryPolicy {

    private final int maxAttempts;
    private final int initialWaitTimeMs;

    /**
     * Constructor for {@link ExponentialPolicy}.
     * @param maxAttempts The maximum attempts.
     * @param initialWaitTimeMs The initial wait time in milliseconds, wait time will grow exponentially with it.
     */
    ExponentialPolicy(int maxAttempts, int initialWaitTimeMs) {
      this.maxAttempts = maxAttempts;
      this.initialWaitTimeMs = initialWaitTimeMs;
    }

    @Override
    public int maxAttempts() {
      return maxAttempts;
    }

    @Override
    public int waitTimeMs(int attempts) {
      checkWaitTimeMsArgs(attempts, maxAttempts);
      return initialWaitTimeMs * (1 << (attempts - 1));
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      ExponentialPolicy other = (ExponentialPolicy) o;
      return maxAttempts == other.maxAttempts && initialWaitTimeMs == other.initialWaitTimeMs;
    }

    @Override
    public int hashCode() {
      return Objects.hash(maxAttempts, initialWaitTimeMs);
    }

    @Override
    public String toString() {
      return "ExponentialPolicy{" + "maxAttempts=" + maxAttempts + ", initialWaitTimeMs=" + initialWaitTimeMs + '}';
    }
  }

  /**
   * The example {@link RetryPolicy} with fixed wait time for each attempt.
   */
  private static class FixedBackoffPolicy implements RetryPolicy {

    private final int maxAttempts;
    private final int waitTimeMs;

    /**
     * Constructor for {@link FixedBackoffPolicy}.
     * @param maxAttempts The maximum attempts.
     * @param waitTimeMs The wait time in milliseconds, wait time will not change with time.
     */
    FixedBackoffPolicy(int maxAttempts, int waitTimeMs) {
      this.maxAttempts = maxAttempts;
      this.waitTimeMs = waitTimeMs;
    }

    @Override
    public int maxAttempts() {
      return maxAttempts;
    }

    @Override
    public int waitTimeMs(int attempts) {
      checkWaitTimeMsArgs(attempts, maxAttempts);
      return waitTimeMs;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      FixedBackoffPolicy other = (FixedBackoffPolicy) o;
      return maxAttempts == other.maxAttempts && waitTimeMs == other.waitTimeMs;
    }

    @Override
    public int hashCode() {
      return Objects.hash(maxAttempts, waitTimeMs);
    }

    @Override
    public String toString() {
      return "FixedBackoffPolicy{" + "maxAttempts=" + maxAttempts + ", waitTimeMs=" + waitTimeMs + '}';
    }
  }
}
