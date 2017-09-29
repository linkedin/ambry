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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Thread responsible for encrypting and decrypting blob content and per-blob keys.
 * Worker Thread listens to a job queue for new jobs and processes it one by one.
 */
class CryptoWorker implements Runnable, Closeable {
  private final BlockingQueue<CryptoJob> jobQueue;
  private final CryptoService cryptoService;
  private final KeyManagementService kms;
  private final AtomicBoolean enabled;
  private volatile boolean endMarkerSeen = false;
  private final String threadName;
  private CountDownLatch shutdownLatch;

  private static final Logger logger = LoggerFactory.getLogger(CryptoWorker.class);

  /**
   * Instantiates {@link CryptoWorker} with the job queue, {@link CryptoService} and {@link KeyManagementService}
   * @param jobQueue the {@link BlockingQueue} that contains the jobs
   * @param cryptoService the {@link CryptoService} to use to encrypt or decrypt
   * @param kms the {@link KeyManagementService} to fetch keys from
   * @param threadName name of the thread
   */
  CryptoWorker(BlockingQueue<CryptoJob> jobQueue, CryptoService cryptoService, KeyManagementService kms,
      String threadName) {
    this.jobQueue = jobQueue;
    this.cryptoService = cryptoService;
    this.kms = kms;
    enabled = new AtomicBoolean(true);
    shutdownLatch = new CountDownLatch(1);
    this.threadName = threadName;
  }

  @Override
  public void run() {
    try {
      while (enabled.get()) {
        try {
          CryptoJob cryptoJob = jobQueue.take();
          if (!(cryptoJob instanceof EndMarkerJob)) {
            cryptoJob.doOperation(cryptoService, kms);
          } else {
            endMarkerSeen = true;
          }
        } catch (InterruptedException e) {
          logger.error("InterruptedException thrown in CryptoWorker");
        }
      }
      if (!endMarkerSeen) {
        closePendingJobs(new GeneralSecurityException("Shutting down EncryptDecrypt Thread"));
      }
    } finally {
      shutdownLatch.countDown();
    }
  }

  /**
   * Shuts down the thread by closing all pending jobs. Any jobs added after close will be ignored
   */
  @Override
  public void close() {
    if (enabled.compareAndSet(true, false)) {
      jobQueue.add(new EndMarkerJob());
    }
    try {
      shutdownLatch.await(1, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      logger.error("InterruptedException thrown while waiting for CryptoWorker to shutdown");
    }
  }

  /**
   * Closes all pending jobs with the given {@code e}
   * @param gse the {@link GeneralSecurityException} that needs to be set while invoking callbacks for the pending jobs
   */
  private void closePendingJobs(GeneralSecurityException gse) {
    CryptoJob cryptoJob = jobQueue.poll();
    while (cryptoJob != null) {
      if ((cryptoJob instanceof EndMarkerJob)) {
        break;
      }
      cryptoJob.closeJob(gse);
      cryptoJob = jobQueue.poll();
    }
  }

  /**
   * End Marker Job to be added to the queue during close. Any pending jobs in the queue until the EndMarker will be
   * invoked with {@link GeneralSecurityException} that the CryptoWorker is being shutdown
   */
  class EndMarkerJob implements CryptoJob {
    @Override
    public void doOperation(CryptoService cryptoService, KeyManagementService kms) {
      throw new IllegalStateException("EndMarkerJob should not be executed");
    }

    @Override
    public void closeJob(GeneralSecurityException gse) {
      throw new IllegalStateException("EndMarkerJob should not be executed");
    }
  }
}


