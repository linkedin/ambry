/**
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
package com.github.ambry.router;

import com.github.ambry.utils.Time;


/**
 * Mocks {@link CryptoJobHandler} to allow disabling clean up of crypto jobs
 */
public class MockCryptoJobHandler extends CryptoJobHandler {
  private final boolean disableCleanup;

  /**
   * Instantiates {@link CryptoJobHandler}
   * @param threadCount total number of worker threads
   * @param cryptoJobTimeoutMs crypto job timeout in ms
   * @param time {@link Time} instance to use
   * @param disableCleanup {@code true} will disable cleanups. {@code false} otherwise
   */
  MockCryptoJobHandler(int threadCount, long cryptoJobTimeoutMs, Time time, boolean disableCleanup) {
    super(threadCount, cryptoJobTimeoutMs, time);
    this.disableCleanup = disableCleanup;
  }

  protected void cleanUpExpiredCryptoJobs() {
    if (!disableCleanup) {
      super.cleanUpExpiredCryptoJobs();
    }
  }
}
