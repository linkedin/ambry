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

import com.github.ambry.utils.Pair;
import com.github.ambry.utils.Time;
import java.io.Closeable;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
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
  private final long cryptoJobTimeoutMs;
  private final AtomicBoolean enabled = new AtomicBoolean(false);
  private static final GeneralSecurityException CLOSED_EXCEPTION =
      new GeneralSecurityException("CryptoJobHandler closed");
  private static final GeneralSecurityException CRYPTO_JOB_TIMEOUT_EXCEPTION =
      new GeneralSecurityException("CryptoJob timeout", new TimeoutException());
  private final ExecutorService executor;
  private final BlockingQueue<Pair<CryptoJob, Long>> pendingCryptoJobs = new LinkedBlockingQueue<>();
  private final Time time;

  private static final Logger logger = LoggerFactory.getLogger(CryptoJobHandler.class);

  /**
   * Instantiates {@link CryptoJobHandler}
   * @param threadCount total number of worker threads
   * @param cryptoJobTimeoutMs crypto job timeout in ms
   * @param time {@link Time} instance to use
   */
  CryptoJobHandler(int threadCount, long cryptoJobTimeoutMs, Time time) {
    this.cryptoJobTimeoutMs = cryptoJobTimeoutMs;
    enabled.set(true);
    executor = Executors.newFixedThreadPool(threadCount);
    this.time = time;
  }

  /**
   * Cleans up expired crypto jobs
   */
  protected void cleanUpExpiredCryptoJobs() {
    Pair<CryptoJob, Long> pendingCryptoJob = pendingCryptoJobs.peek();
    while (pendingCryptoJob != null) {
      if (pendingCryptoJob.getFirst().isComplete()) {
        // already completed job
        pendingCryptoJobs.poll();
      } else if (time.milliseconds() - pendingCryptoJob.getSecond() > cryptoJobTimeoutMs) {
        // crypto job timed out
        pendingCryptoJobs.poll();
        pendingCryptoJob.getFirst().completeJob(null, CRYPTO_JOB_TIMEOUT_EXCEPTION);
      } else {
        break;
      }
      pendingCryptoJob = pendingCryptoJobs.peek();
    }
  }

  /**
   * Submits new job to the {@link CryptoJobHandler}
   * @param cryptoJob the {@link CryptoJob} that needs to be executed
   */

  void submitJob(CryptoJob cryptoJob) {
    if (enabled.get()) {
      executor.execute(cryptoJob);
      pendingCryptoJobs.add(new Pair(cryptoJob, time.milliseconds()));
    }
  }

  /**
   * Shuts down the ExecutorService. Any new jobs submitted after close are ignored. Exception will be set in the callback
   * for all the pending jobs
   */
  public void close() {
    if (enabled.compareAndSet(true, false)) {
      List<Runnable> pendingTasks = executor.shutdownNow();
      for (Runnable task : pendingTasks) {
        if (task instanceof CryptoJob) {
          ((CryptoJob) task).completeJob(null, CLOSED_EXCEPTION);
        } else {
          logger.error("Unknown type of job seen : " + task.getClass());
        }
      }
    }
  }
}
