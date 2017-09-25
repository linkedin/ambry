package com.github.ambry.router;

import com.github.ambry.utils.Utils;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Responsible for handling crypto jobs. {@link Router} instantiates this {@link CryptoJobHandler} and gives a
 * reference of the same to PutManager, GetBlobOperation and GetBlobInfoOperation which will add jobs via
 * {@link #submitJob(CryptoJob)}. On close, all pending jobs will be processed(either successfully or exception is set)
 * and any new jobs submitted after close will be ignored.
 */
class CryptoJobHandler implements Closeable{
  private final BlockingQueue<CryptoJob> jobQueue;
  private final List<CryptoWorker> workerThreads;
  private AtomicBoolean enabled;
  private AtomicInteger jobCount = new AtomicInteger(0);

  private static final Logger logger = LoggerFactory.getLogger(CryptoJobHandler.class);

  /**
   * Instantiates {@link CryptoJobHandler} with the job queue, {@link CryptoService} and {@link KeyManagementService}
   * @param cryptoService the {@link CryptoService} to use to encrypt or decrypt
   * @param kms the {@link KeyManagementService} to fetch keys from
   */
  CryptoJobHandler(CryptoService cryptoService, KeyManagementService kms, int workerCount) {
    this.jobQueue = new LinkedBlockingQueue<>();
    this.workerThreads = new ArrayList<>();
    for (int i = 0; i < workerCount; i++) {
      workerThreads.add(new CryptoWorker(jobQueue, cryptoService, kms, "CryptoThread_" + i));
    }
    this.enabled = new AtomicBoolean(false);
  }

  /**
   * Starts up {@link CryptoJobHandler} by spinning up multiple {@link CryptoWorker} threads to assist in executing
   * {@link CryptoJob}s submitted to {@link CryptoJobHandler} via {@link #submitJob(CryptoJob)}
   */
  void startup() {
    enabled.set(true);
    for (CryptoWorker worker : workerThreads) {
      Utils.newThread(worker, true).start();
    }
  }

  /**
   * Submits new job to the {@link CryptoJobHandler}
   * @param cryptoJob the {@link CryptoJob} that needs to be executed
   */
  void submitJob(CryptoJob cryptoJob) {
    if (enabled.get()) {
      jobQueue.add(cryptoJob);
    }
  }

  /**
   * Shuts down the job handler. Any new jobs submitted after close are ignored. All the pending jobs will get processed
   * for sure (either exception will be set or will get processed in usual manner)
   */
  public void close() {
    if (enabled.compareAndSet(true, false)) {
      for (CryptoWorker worker : workerThreads) {
        worker.close();
      }
    }
  }
}
