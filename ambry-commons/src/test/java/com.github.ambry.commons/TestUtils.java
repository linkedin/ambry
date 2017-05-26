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
package com.github.ambry.commons;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


class TestUtils {

  /**
   * Awaits on the passed-in {@link CountDownLatch}. If times out throws an exception.
   * @param latch The latch to await on.
   * @param timeoutMs Timeout in millisecond.
   * @throws Exception If awaits for more than the specified time, throw a {@link TimeoutException}.
   */
  static void awaitLatchOrTimeout(CountDownLatch latch, long timeoutMs) throws Exception {
    if (!latch.await(timeoutMs, TimeUnit.MILLISECONDS)) {
      throw new TimeoutException("Too long time to complete operation.");
    }
  }
}
