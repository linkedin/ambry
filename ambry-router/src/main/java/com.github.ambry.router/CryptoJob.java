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

/**
 * CryptoJob representing the job that needs processing by {@link CryptoJobHandler}
 */
public interface CryptoJob<T> extends Runnable {

  /**
   * @return {@code true} if the job is complete. {@code false} otherwise
   */
  boolean isComplete();

  /**
   * Completes the job by invoking the callback with the result or exception
   * @param result the result that needs to be set in the callback. Could be {@code null}
   * @param e {@link Exception} to be set in the callback. Could be {@code null}
   */
  void completeJob(T result, Exception e);
}
