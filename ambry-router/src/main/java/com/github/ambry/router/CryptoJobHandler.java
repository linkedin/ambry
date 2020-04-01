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

import java.io.Closeable;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Responsible for handling crypto jobs. {@link Router} instantiates this {@link CryptoJobHandler} and gives a
 * reference of the same to PutManager, GetBlobOperation and GetBlobInfoOperation which will add jobs via
 * {@link #submitJob(CryptoJob)}. On close, all pending jobs will be processed(either successfully or exception is set)
 * and any new jobs submitted after close will be ignored.
 */
class CryptoJobHandler implements Closeable {
  private final AtomicBoolean enabled = new AtomicBoolean(false);
  private static final GeneralSecurityException CLOSED_EXCEPTION =
      new GeneralSecurityException("CryptoJobHandler closed");
  private ExecutorService scheduler;

  private static final Logger logger = LoggerFactory.getLogger(CryptoJobHandler.class);

  /**
   * Instantiates {@link CryptoJobHandler}
   */
  CryptoJobHandler(int threadCount) {
    enabled.set(true);
    scheduler = Executors.newFixedThreadPool(threadCount);
  }

  /**
   * Submits new job to the {@link CryptoJobHandler}
   * @param cryptoJob the {@link CryptoJob} that needs to be executed
   */
  void submitJob(CryptoJob cryptoJob) {
    if (enabled.get()) {
      scheduler.execute(cryptoJob);
    }
  }

  /**
   * Shuts down the ExecutorService. Any new jobs submitted after close are ignored. Exception will be set in the callback
   * for all the pending jobs
   */
  public void close() {
    if (enabled.compareAndSet(true, false)) {
      List<Runnable> pendingTasks = scheduler.shutdownNow();
      for (Runnable task : pendingTasks) {
        if (task instanceof CryptoJob) {
          ((CryptoJob) task).closeJob(CLOSED_EXCEPTION);
        } else {
          logger.error("Unknown type of job seen : " + task.getClass());
        }
      }
      try {
        scheduler.awaitTermination(1000, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        logger.error("Unexpected exception while waiting for crypto jobs to terminate", e);
      }
    }
  }
}
