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

package com.github.ambry.commons;

import com.github.ambry.utils.Utils;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.utils.Utils.*;
import static java.util.concurrent.TimeUnit.*;


/**
 * A collection of utilities that expand the usage of {@link CompletableFuture}. Inspired from
 * https://github.com/apache/flink/blob/master/flink-core/src/main/java/org/apache/flink/util/concurrent/FutureUtils.java
 */
public class FutureUtils {

  private static final CompletableFuture<Void> COMPLETED_VOID_FUTURE = CompletableFuture.completedFuture(null);
  private static final String TIMEOUT_SCHEDULER = "CompletableFutureTimeOut";

  private static final ScheduledExecutorService scheduler = Utils.newScheduler(1, TIMEOUT_SCHEDULER, false);
  private static final Logger logger = LoggerFactory.getLogger(FutureUtils.class);

  /**
   * @return a completed future of type {@link Void}
   */
  public static CompletableFuture<Void> completedVoidFuture() {
    return COMPLETED_VOID_FUTURE;
  }

  /**
   * Fakes asynchronous execution by immediately executing the operation and completing the supplied future
   * either normally or exceptionally.
   * @param operation to executed
   * @param <T> type of the result
   */
  public static <T> void completeFromCallable(CompletableFuture<T> future, Callable<T> operation) {
    try {
      future.complete(operation.call());
    } catch (Exception e) {
      future.completeExceptionally(e);
    }
  }

  /**
   * Retry the given operation the given number of times in case of a failure.
   * @param <T> type of the result
   * @param operation to executed
   * @param retries if the operation failed
   * @param retryPredicate Predicate to test whether an exception is retryable
   * @param retryErrorConsumer {@link Consumer} of errors during retry operation
   * @param finalErrorConsumer {@link Consumer} of final error after all retries are completed
   * @return Future containing either the result of the operation or a {@link Exception} if error occurred.
   */
  public static <T> CompletableFuture<T> retry(final Supplier<CompletableFuture<T>> operation, final int retries,
      Predicate<Throwable> retryPredicate, Consumer<Throwable> retryErrorConsumer,
      Consumer<Throwable> finalErrorConsumer) {
    final CompletableFuture<T> resultFuture = new CompletableFuture<>();
    retryOperation(resultFuture, operation, retries, retryPredicate, retryErrorConsumer);
    resultFuture.whenComplete((t, throwable) -> {
      if (throwable != null) {
        finalErrorConsumer.accept(throwable);
      }
    });
    return resultFuture;
  }

  /**
   * Retry the given operation the given number of times in case of a failure.
   * @param <T> type of the result
   * @param operation to executed
   * @param retries if the operation failed
   * @param retryPredicate Predicate to test whether an exception is retryable
   * @param retryDelayFromException function to get retry delay from the exception if present
   * @param scheduledExecutor executor to schedule retries
   * @return Future containing either the result of the operation or a {@link Exception} if error occurred.
   */
  public static <T> CompletableFuture<T> retry(final Supplier<CompletableFuture<T>> operation, final int retries,
      Duration retryDelay, Predicate<Throwable> retryPredicate, Function<Throwable, Integer> retryDelayFromException,
      ScheduledExecutorService scheduledExecutor) {
    final CompletableFuture<T> resultFuture = new CompletableFuture<>();
    retryOperationWithDelay(resultFuture, operation, retries, retryDelay, retryPredicate, retryDelayFromException,
        scheduledExecutor);
    return resultFuture;
  }

  /**
   * Helper method which retries the provided operation in case of a failure.
   * @param <T> type of the future's result
   * @param resultFuture to complete
   * @param operation to retry
   * @param retries until giving up
   * @param retryPredicate Predicate to test whether an exception is retryable
   * @param retryErrorConsumer {@link Consumer} of errors during retry operation
   */
  private static <T> void retryOperation(final CompletableFuture<T> resultFuture,
      final Supplier<CompletableFuture<T>> operation, final int retries, Predicate<Throwable> retryPredicate,
      Consumer<Throwable> retryErrorConsumer) {

    if (!resultFuture.isDone()) {
      final CompletableFuture<T> operationFuture = operation.get();

      operationFuture.whenCompleteAsync((t, throwable) -> {
        if (throwable != null) {
          if (throwable instanceof CancellationException) {
            resultFuture.completeExceptionally(throwable);
          } else {
            throwable = extractFutureExceptionCause(throwable);
            if (!retryPredicate.test(throwable)) {
              resultFuture.completeExceptionally(throwable);
            } else if (retries > 0) {
              retryErrorConsumer.accept(throwable);
              retryOperation(resultFuture, operation, retries - 1, retryPredicate, retryErrorConsumer);
            } else {
              resultFuture.completeExceptionally(throwable);
            }
          }
        } else {
          resultFuture.complete(t);
        }
      });
    }
  }

  /**
   * @param <T> type of the future's result
   * @param resultFuture to complete
   * @param operation to retry
   * @param retries until giving up
   * @param retryDelay duration of retries
   * @param retryPredicate Predicate to test whether an exception is retryable
   * @param retryDelayFromException function to get retry delay from the exception if present
   * @param scheduler executor to schedule retries
   */
  private static <T> void retryOperationWithDelay(final CompletableFuture<T> resultFuture,
      final Supplier<CompletableFuture<T>> operation, final int retries, Duration retryDelay,
      final Predicate<Throwable> retryPredicate, Function<Throwable, Integer> retryDelayFromException,
      final ScheduledExecutorService scheduler) {

    if (!resultFuture.isDone()) {
      final CompletableFuture<T> operationResultFuture = operation.get();

      operationResultFuture.whenComplete((t, throwable) -> {
        if (throwable != null) {
          if (throwable instanceof CancellationException) {
            resultFuture.completeExceptionally(throwable);
          } else {
            throwable = extractFutureExceptionCause(throwable);
            if (!retryPredicate.test(throwable)) {
              resultFuture.completeExceptionally(throwable);
            } else if (retries > 0) {
              long retryDelayFromExceptionMs = retryDelayFromException.apply(throwable);
              long retryDelayMillis = retryDelayFromExceptionMs > 0 ? retryDelayFromExceptionMs : retryDelay.toMillis();
              if (scheduler == null) {
                if (retryDelayMillis > 0) {
                  logger.warn("No scheduler for delay execution. waitTimeMs: {}", retryDelayMillis);
                  retryOperationWithDelay(resultFuture, operation, retries - 1, retryDelay, retryPredicate,
                      retryDelayFromException, scheduler);
                }
              } else {
                final ScheduledFuture<?> scheduledFuture = scheduler.schedule(
                    () -> retryOperationWithDelay(resultFuture, operation, retries - 1, retryDelay, retryPredicate,
                        retryDelayFromException, scheduler), retryDelayMillis, MILLISECONDS);

                resultFuture.whenComplete((innerT, innerThrowable) -> scheduledFuture.cancel(false));
              }
            } else {
              resultFuture.completeExceptionally(throwable);
            }
          }
        } else {
          resultFuture.complete(t);
        }
      });
    }
  }

  /**
   * Returns an exceptionally completed {@link CompletableFuture}.
   * @param cause to complete the future with
   * @param <T> type of the future
   * @return An exceptionally completed CompletableFuture
   */
  public static <T> CompletableFuture<T> completedExceptionally(Throwable cause) {
    CompletableFuture<T> result = new CompletableFuture<>();
    result.completeExceptionally(cause);

    return result;
  }

  /**
   * Times the given future out after the timeout.
   * @param <T> type of the given future
   * @param future to time out
   * @param duration Duration after which future is timed out
   * @return The timeout enriched future
   */
  public static <T> CompletableFuture<T> orTimeout(CompletableFuture<T> future, Duration duration) {
    final CompletableFuture<T> timeout = failAfter(duration);
    return future.applyToEither(timeout, Function.identity());
  }

  /**
   * Returns a completable future which times out after a given duration.
   * @param duration for which a timeout future is needed
   * @param <T> type of the given future
   * @return A completable future which times out after a given duration
   */
  public static <T> CompletableFuture<T> failAfter(Duration duration) {
    final CompletableFuture<T> promise = new CompletableFuture<>();
    scheduler.schedule(() -> {
      final TimeoutException ex = new TimeoutException("Timeout after " + duration);
      return promise.completeExceptionally(ex);
    }, duration.toMillis(), MILLISECONDS);
    return promise;
  }
}
