package com.github.ambry.commons;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
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
   * @param userCallback The user defined {@link Callback} to be executed after success or if all retry attempts failed.
   */
  public <T> void runWithRetries(RetryPolicy policy, Consumer<Callback<T>> call, Callback<T> userCallback) {
    recursiveAsyncRetry(call, policy, userCallback, 0);
  }

  /**
   * Recursively retrying on the task call with specific policy and user defined callback.
   * @param call The function call will be executed and retried with failure.
   * @param policy The {@link RetryPolicy} to schedule retrying.
   * @param userCallback The user defined {@link Callback} to be executed after all retry attempts failed.
   * @param attempts The number of retries has been attempted.
   */
  private <T> void recursiveAsyncRetry(Consumer<Callback<T>> call, RetryPolicy policy, Callback<T> userCallback,
      int attempts) {
    call.accept((result, exception) -> {
      if (exception != null) {
        int currAttempts = attempts + 1;
        if (currAttempts < policy.maxAttempts() && isRetriable(exception)) {
          int waitTimeMs = policy.waitTimeMs(currAttempts);
          logger.info("{} of {} attempts failed, will keep retrying after a {} ms backoff. exception='{}'",
              currAttempts, policy.maxAttempts(), waitTimeMs, exception);
          scheduler.schedule(() -> recursiveAsyncRetry(call, policy, userCallback, currAttempts), waitTimeMs,
              TimeUnit.MILLISECONDS);
        } else {
          logger.info("{} of {} attempts failed, completing operation. exception='{}'", currAttempts,
              policy.maxAttempts(), exception);
          userCallback.onCompletion(null, exception);
        }
      } else {
        userCallback.onCompletion(result, null);
      }
    });
  }

  /**
   * Decide on a specific {@link Throwable} can be retried or not.
   * @param throwable The {@link Throwable} thrown in execution of retrying.
   */
  private static boolean isRetriable(Throwable throwable) {
    return true;
  }
}
