package com.github.ambry.commons;

/**
 * The {@link RetryPolicy} defines the specific number of  maximum attempts, wait time in milliseconds and maximum
 * time in milliseconds for timeout. Users can provide their own implementations of policy for different
 * needs. Two implementations: {@link RetryPolicies.ExponentialPolicy} and {@link RetryPolicies.FixedBackoffPolicy} are
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
