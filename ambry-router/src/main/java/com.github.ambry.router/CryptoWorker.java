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

import com.github.ambry.commons.BlobId;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Thread responsible for encrypting and decrypting blob content and per blob keys. Worker Thread listens to a job queue
 * for new jobs and processes it one by one.
 */
public class CryptoWorker implements Runnable {
  private final BlockingQueue<CryptoJob> jobQueue;
  private final CryptoService cryptoService;
  private final KeyManagementService kms;
  private final AtomicBoolean enabled;
  private CountDownLatch shutdownLatch;

  private static final Logger logger = LoggerFactory.getLogger(CryptoWorker.class);

  /**
   * Instantiates {@link CryptoWorker} with the job queue, {@link CryptoService} and {@link KeyManagementService}
   * @param jobQueue the {@link BlockingQueue} that contains the jobs
   * @param cryptoService the {@link CryptoService} to use to encrypt or decrypt
   * @param kms the {@link KeyManagementService} to fetch keys from
   */
  CryptoWorker(BlockingQueue<CryptoJob> jobQueue, CryptoService cryptoService, KeyManagementService kms) {
    this.jobQueue = jobQueue;
    this.cryptoService = cryptoService;
    this.kms = kms;
    enabled = new AtomicBoolean(false);
  }

  @Override
  public void run() {
    enabled.set(true);
    shutdownLatch = new CountDownLatch(1);
    try {
      while (enabled.get()) {
        try {
          CryptoJob cryptoJob = jobQueue.poll(10, TimeUnit.MILLISECONDS);
          if (cryptoJob != null) {
            cryptoJob.doOperation(cryptoService, kms);
          }
        } catch (InterruptedException e) {
          logger.error("InterruptedException thrown in CryptoWorker");
        }
      }
    } finally {
      shutdownLatch.countDown();
    }
  }

  /**
   * Shuts down the thread by closing all pending jobs
   */
  void close() {
    if (enabled.compareAndSet(true, false)) {
      logger.info("Closing CryptoWorker");
      closePendingJobs(new GeneralSecurityException("Shutting down EncryptDecrypt Thread"));
    }
    if (shutdownLatch != null) {
      try {
        shutdownLatch.await(1, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        logger.error("InterruptedException thrown while waiting for CryptoWorker to shutdown");
      }
    }
  }

  /**
   * Closes all pending jobs with the given {@code e}
   * @param gse the {@link GeneralSecurityException} that needs to be set while invoking callbacks for the pending jobs
   */
  private void closePendingJobs(GeneralSecurityException gse) {
    CryptoJob cryptoJob = jobQueue.poll();
    while (cryptoJob != null) {
      cryptoJob.closeJob(gse);
      cryptoJob = jobQueue.poll();
    }
  }

  /**
   * CryptoJob representing the job that needs processing
   */
  static abstract class CryptoJob {
    protected final BlobId blobId;
    protected final ByteBuffer input;

    CryptoJob(BlobId blobId, ByteBuffer input) {
      this.blobId = blobId;
      this.input = input;
    }

    /**
     * Processing of the job using {@code cryptoService} and {@code kms}.
     * @param cryptoService the {@link CryptoService} to assist in encrypting and decrypting
     * @param kms the {@link KeyManagementService} to fetch keys from
     */
    abstract void doOperation(CryptoService cryptoService, KeyManagementService kms);

    /**
     * Close the job with the given {@code gse}
     * @param gse the {@link GeneralSecurityException} that needs to be set while invoking callback for the job
     */
    abstract void closeJob(GeneralSecurityException gse);
  }

  /**
   * Callback to invoke for encrypt jobs
   */
  interface EncryptCallback extends Callback<EncryptJobResult> {
    /**
     * Callback to be invoked on completion of an encryption job.
     * @param result {@link EncryptJobResult} representing the result of an encrypt job
     * @param e {@link Exception} thrown while encrypting
     */
    void onCompletion(EncryptJobResult result, Exception e);
  }

  /**
   * Callback to invoke for decrypt jobs
   */
  interface DecryptCallback extends Callback<DecryptJobResult> {
    /**
     * Callback to be invoked on completion of an decryption job.
     * @param result {@link DecryptJobResult} representing the result of an decrypt job
     * @param e {@link Exception} thrown while decrypting
     */
    void onCompletion(DecryptJobResult result, Exception e);
  }

  /**
   * Class representing encrypt job result
   */
  static class EncryptJobResult {
    private final BlobId blobId;
    private final ByteBuffer encryptedKey;
    private final ByteBuffer encryptedContent;

    EncryptJobResult(BlobId blobId, ByteBuffer encryptedKey, ByteBuffer encryptedContent) {
      this.blobId = blobId;
      this.encryptedKey = encryptedKey;
      this.encryptedContent = encryptedContent;
    }

    BlobId getBlobId() {
      return blobId;
    }

    ByteBuffer getEncryptedKey() {
      return encryptedKey;
    }

    ByteBuffer getEncryptedContent() {
      return encryptedContent;
    }
  }

  /**
   * Class respresenting decrypt job result
   */
  static class DecryptJobResult {
    private final BlobId blobId;
    private final ByteBuffer decryptedContent;

    DecryptJobResult(BlobId blobId, ByteBuffer decryptedContent) {
      this.blobId = blobId;
      this.decryptedContent = decryptedContent;
    }

    BlobId getBlobId() {
      return blobId;
    }

    ByteBuffer getDecryptedContent() {
      return decryptedContent;
    }
  }

  /**
   * Class representing an encrypt Job.
   */
  static class EncryptJob extends CryptoJob {
    private final Object perBlobKey;
    private final EncryptCallback callback;

    /**
     * Instantiates {@link EncryptJob} with {@link BlobId}, content to encrypt, perBlobKey and the {@link EncryptCallback}
     * @param blobId the {@link BlobId} for which encryption is requested
     * @param toEncrypt {@link ByteBuffer} to be encrypted
     * @param perBlobKey per blob key to use to encrypt the blob content
     * @param callback {@link EncryptCallback} to be invoked on completion
     */
    EncryptJob(BlobId blobId, ByteBuffer toEncrypt, Object perBlobKey, EncryptCallback callback) {
      super(blobId, toEncrypt);
      this.perBlobKey = perBlobKey;
      this.callback = callback;
    }

    /**
     * Steps to be performed on encryption
     * 1. Encrypt blob content using perBlobKey
     * 2. Fetch ContainerKey from kms for the given blob
     * 3. Encrypt perBlobKey using containerKey
     * 4. Invoke callback with the encryptedKey and encryptedContent
     * @param cryptoService the {@link CryptoService} to assist in encrypting and decrypting
     * @param kms the {@link KeyManagementService} to fetch keys from
     */
    @Override
    void doOperation(CryptoService cryptoService, KeyManagementService kms) {
      try {
        ByteBuffer encryptedContent = cryptoService.encrypt(input, perBlobKey);
        Object containerKey = kms.getKey(blobId.getAccountId(), blobId.getContainerId());
        ByteBuffer encryptedKey = cryptoService.encryptKey(perBlobKey, containerKey);
        callback.onCompletion(new EncryptJobResult(blobId, encryptedKey, encryptedContent), null);
      } catch (Exception e) {
        callback.onCompletion(new EncryptJobResult(blobId, null, null), e);
      }
    }

    @Override
    void closeJob(GeneralSecurityException gse) {
      callback.onCompletion(new EncryptJobResult(blobId, null, null), gse);
    }
  }

  /**
   * Class representing an decrypt Job.
   */
  static class DecryptJob extends CryptoJob {
    private final ByteBuffer encryptedKey;
    private final DecryptCallback callback;

    /**
     * Instantiates {@link DecryptJob} with {@link BlobId}, key to be decrypted, content to be decrypted and the
     * {@link DecryptCallback}
     * @param blobId the {@link BlobId} for which decryption is requested
     * @param encryptedKey encrypted per blob key
     * @param encryptedContent encrypted blob content
     * @param callback {@link DecryptCallback} to be invoked on completion
     */
    DecryptJob(BlobId blobId, ByteBuffer encryptedKey, ByteBuffer encryptedContent, DecryptCallback callback) {
      super(blobId, encryptedContent);
      this.encryptedKey = encryptedKey;
      this.callback = callback;
    }

    /**
     * Steps to be performed on decryption
     * 1. Fetch ContainerKey from kms for the given blob
     * 2. Decrypt encryptedKey using containerKey to obtain perBlobKey
     * 3. Decrypt encryptedContent using perBlobKey
     * 4. Invoke callback with the decryptedContent
     * @param cryptoService the {@link CryptoService} to assist in encrypting and decrypting
     * @param kms the {@link KeyManagementService} to fetch keys from
     */
    @Override
    void doOperation(CryptoService cryptoService, KeyManagementService kms) {
      try {
        Object containerKey = kms.getKey(blobId.getAccountId(), blobId.getContainerId());
        Object perBlobKey = cryptoService.decryptKey(encryptedKey, containerKey);
        ByteBuffer decryptedContent = cryptoService.decrypt(input, perBlobKey);
        callback.onCompletion(new DecryptJobResult(blobId, decryptedContent), null);
      } catch (Exception e) {
        callback.onCompletion(new DecryptJobResult(blobId, null), e);
      }
    }

    @Override
    void closeJob(GeneralSecurityException gse) {
      callback.onCompletion(new DecryptJobResult(blobId, null), gse);
    }
  }
}


