/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.router;

import java.util.concurrent.atomic.AtomicBoolean;


/**
 * CryptoJob representing the job that needs processing by {@link CryptoJobHandler}
 */
public abstract class CryptoJob<T> implements Runnable {
  private AtomicBoolean isComplete = new AtomicBoolean(false);
  private Callback callback;
  private final AtomicBoolean callbackInvoked = new AtomicBoolean(false);

  CryptoJob(Callback callback) {
    this.callback = callback;
  }

  /**
   * @return {@code true} if the job is complete. {@code false} otherwise
   */
  boolean isComplete() {
    return isComplete.get();
  }

  /**
   * Completes the job by invoking the callback with the result or exception
   * @param result the result that needs to be set in the callback. Could be {@code null}
   * @param e {@link Exception} to be set in the callback. Could be {@link null}
   */
  void completeJob(T result, Exception e) {
    if (callbackInvoked.compareAndSet(false, true)) {
      callback.onCompletion(result, e);
      isComplete.set(true);
    }
  }
}
