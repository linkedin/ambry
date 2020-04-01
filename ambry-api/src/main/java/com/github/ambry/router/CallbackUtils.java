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
 *
 */

package com.github.ambry.router;

import com.github.ambry.utils.AsyncOperationTracker;
import com.github.ambry.utils.ThrowingConsumer;


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
        if (exception == null) {
          successAction.accept(result);
        }
      } catch (Exception e) {
        asyncOperationTracker.markCallbackProcessingError();
        exception = e;
      } finally {
        if (exception != null) {
          failureCallback.onCompletion(null, exception);
        }
        asyncOperationTracker.markCallbackProcessingEnd();
      }
    };
  }
}
