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

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.github.ambry.utils.Utils.*;


/**
 * A collection of utilities that expand the usage of {@link CompletableFuture}. Inspired from
 * https://github.com/apache/flink/blob/master/flink-core/src/main/java/org/apache/flink/util/concurrent/FutureUtils.java
 */
public class FutureUtils {

  /**
   * Retry the given operation the given number of times in case of a failure.
   *
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

      resultFuture.whenComplete((t, throwable) -> operationFuture.cancel(false));
    }
  }
}
