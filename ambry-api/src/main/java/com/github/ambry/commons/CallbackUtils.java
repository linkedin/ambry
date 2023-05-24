/*
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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
 *
 */

package com.github.ambry.commons;

import com.github.ambry.utils.AsyncOperationTracker;
import com.github.ambry.utils.ThrowingConsumer;
import com.github.ambry.utils.Utils;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;


/**
 * Utilities for working with {@link Callback}.
 */
public class CallbackUtils {
  /**
   * Create a {@link Callback} that handles common tasks like updating metrics via a {@link AsyncOperationTracker} and
   * handling errors that may occur in the callback body. It is meant to be a part of multi-step callback chain and
   * handle intermediate failures in a consistent way.
   * @param <T> the result type that the callback accepts.
   * @param asyncOperationTracker the {@link AsyncOperationTracker} to use to update metrics for an async call. The
   *                              operation is marked as started right before the callback is constructed.
   * @param failureCallback the callback to call if this callback was called with an exception or if the
   *                        {@code successAction} call throws an exception.
   * @param successAction the action to take when the callback is called successfully. The result of the callback is
   *                      provided to this consumer. This consumer is allowed to throw an exception, in which case the
   *                      {@code failureCallback} will be called.
   * @return the managed {@link Callback}.
   */
  public static <T> Callback<T> chainCallback(AsyncOperationTracker asyncOperationTracker, Callback<?> failureCallback,
      ThrowingConsumer<? super T> successAction) {
    asyncOperationTracker.markOperationStart();
    return (result, exception) -> {
      try {
        asyncOperationTracker.markOperationEnd();
        if (exception == null && successAction != null) {
          successAction.accept(result);
        }
      } catch (Exception e) {
        asyncOperationTracker.markCallbackProcessingError();
        exception = e;
      } finally {
        if (exception != null && failureCallback != null) {
          failureCallback.onCompletion(null, exception);
        }
        asyncOperationTracker.markCallbackProcessingEnd();
      }
    };
  }

  /**
   * Call an Ambry callback when a {@link CompletionStage} is completed, appropriately handling exception wrapping
   * by the future implementation.
   * @param completionStage the {@link CompletionStage} to attach to.
   * @param callback the {@link Callback} to call upon completion.
   * @param <T> the type of the future and callback.
   */
  public static <T> void callCallbackAfter(CompletionStage<T> completionStage, Callback<T> callback) {
    completionStage.whenComplete(
        (result, throwable) -> callback.onCompletion(result, Utils.extractFutureExceptionCause(throwable)));
  }

  /**
   * Create a callback that would complete the given {@link CompletableFuture} when the corresponding result
   * comes to callback.
   * @param future The given {@link CompletableFuture} to complete.
   * @param <T> The result's type.
   * @return
   */
  public static <T> Callback<T> fromCompletableFuture(CompletableFuture<T> future) {
    return (result, exception) -> {
      if (exception != null) {
        future.completeExceptionally(exception);
      } else {
        future.complete(result);
      }
    };
  }
}
