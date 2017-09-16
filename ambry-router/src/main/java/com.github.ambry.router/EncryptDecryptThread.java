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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Thread responsible for encrypting and decrypting blob content and per blob keys. Thread listens to a job queue
 * for new jobs and processes it one by one.
 */
public class EncryptDecryptThread implements Runnable {
  private final BlockingQueue<JobInfo> jobQueue;
  private final CryptoService cryptoService;
  private final KeyManagementService kms;
  private final AtomicBoolean enabled;
  private final Lock lock;
  private CountDownLatch shutdownLatch;

  private static final Logger logger = LoggerFactory.getLogger(EncryptDecryptThread.class);

  /**
   * Instantiates {@link EncryptDecryptThread} with the job queue, {@link CryptoService} and {@link KeyManagementService}
   * @param jobQueue the {@link BlockingQueue} that contains the jobs
   * @param cryptoService the {@link CryptoService} to use to encrypt or decrypt
   * @param kms the {@link KeyManagementService} to fetch keys from
   */
  EncryptDecryptThread(BlockingQueue<JobInfo> jobQueue, CryptoService cryptoService, KeyManagementService kms) {
    this.jobQueue = jobQueue;
    this.cryptoService = cryptoService;
    this.kms = kms;
    enabled = new AtomicBoolean(false);
    lock = new ReentrantLock();
  }

  @Override
  public void run() {
    enabled.set(true);
    shutdownLatch = new CountDownLatch(1);
    try {
      while (enabled.get()) {
        lock.lock();
        try {
          JobInfo jobInfo = jobQueue.poll(10, TimeUnit.MILLISECONDS);
          if (jobInfo != null) {
            jobInfo.doOperation(cryptoService, kms);
          }
        } catch (InterruptedException e) {
          logger.error("InterruptedException thrown in EncryptDecryptThread");
        } finally {
          lock.unlock();
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
    lock.lock();
    try {
      if (enabled.compareAndSet(true, false)) {
        logger.info("Closing EncryptDecryptThread");
        closePendingJobs(new GeneralSecurityException("Shutting down EncryptDecrypt Thread"));
      }
    } finally {
      lock.unlock();
    }
    if (shutdownLatch != null) {
      try {
        shutdownLatch.await(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        logger.error("InterruptedException thrown while waiting for EncryptDecryptThread to shutdown");
      }
    }
  }

  /**
   * Closes all pending jobs with the given {@code e}
   * @param gse the {@link GeneralSecurityException} that needs to be set while invoking callbacks for the pending jobs
   */
  private void closePendingJobs(GeneralSecurityException gse) {
    JobInfo jobInfo = jobQueue.poll();
    while (jobInfo != null) {
      jobInfo.closeJob(gse);
      jobInfo = jobQueue.poll();
    }
  }

  /**
   * JobInfo representing the job that needs processing
   */
  static abstract class JobInfo {
    protected final BlobId blobId;
    protected final ByteBuffer input;

    JobInfo(BlobId blobId, ByteBuffer input) {
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
  interface EncryptCallBack {
    /**
     * Callback to be invoked on completion of an encryption job.
     * @param blobId {@link BlobId} for which encryption was requested
     * @param encryptedKey {@link ByteBuffer} representing the encrypted per blob key
     * @param encryptedContent {@link ByteBuffer} representing encrypted content
     * @param gse {@link GeneralSecurityException} thrown while encrypting
     */
    void onCompletion(BlobId blobId, ByteBuffer encryptedKey, ByteBuffer encryptedContent,
        GeneralSecurityException gse);
  }

  /**
   * Callback to invoke for decrypt jobs
   */
  interface DecryptCallBack {
    /**
     * Callback to be invoked on completion of an decryption job.
     * @param blobId {@link BlobId} for which encryption was requested
     * @param decryptedContent {@link ByteBuffer} representing decrypted content
     * @param gse {@link GeneralSecurityException} thrown while decrypting
     */
    void onCompletion(BlobId blobId, ByteBuffer decryptedContent, GeneralSecurityException gse);
  }

  /**
   * Class representing an encrypt Job.
   */
  static class EncryptJobInfo extends JobInfo {
    private final Object perBlobKey;
    private final EncryptCallBack callBack;

    /**
     * Instantiates {@link EncryptJobInfo} with {@link BlobId}, content to encrypt, perBlobKey and the {@link EncryptCallBack}
     * @param blobId the {@link BlobId} for which encryption is requested
     * @param toEncrypt {@link ByteBuffer} to be encrypted
     * @param perBlobKey per blob key to use to encrypt the blob content
     * @param callBack {@link EncryptCallBack} to be invoked on completion
     */
    EncryptJobInfo(BlobId blobId, ByteBuffer toEncrypt, Object perBlobKey, EncryptCallBack callBack) {
      super(blobId, toEncrypt);
      this.perBlobKey = perBlobKey;
      this.callBack = callBack;
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
        callBack.onCompletion(blobId, encryptedKey, encryptedContent, null);
      } catch (GeneralSecurityException e) {
        callBack.onCompletion(blobId, null, null, e);
      }
    }

    @Override
    void closeJob(GeneralSecurityException gse) {
      callBack.onCompletion(blobId, null, null, gse);
    }
  }

  /**
   * Class representing an decrypt Job.
   */
  static class DecryptJobInfo extends JobInfo {
    private final ByteBuffer encryptedKey;
    private final DecryptCallBack callBack;

    /**
     * Instantiates {@link DecryptJobInfo} with {@link BlobId}, key to be decrypted, content to be decrypted and the
     * {@link DecryptCallBack}
     * @param blobId the {@link BlobId} for which decryption is requested
     * @param encryptedKey encrypted per blob key
     * @param encryptedContent encrypted blob content
     * @param callBack {@link DecryptCallBack} to be invoked on completion
     */
    DecryptJobInfo(BlobId blobId, ByteBuffer encryptedKey, ByteBuffer encryptedContent, DecryptCallBack callBack) {
      super(blobId, encryptedContent);
      this.encryptedKey = encryptedKey;
      this.callBack = callBack;
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
        callBack.onCompletion(blobId, decryptedContent, null);
      } catch (GeneralSecurityException e) {
        callBack.onCompletion(blobId, null, e);
      }
    }

    @Override
    void closeJob(GeneralSecurityException gse) {
      callBack.onCompletion(blobId, null, gse);
    }
  }
}


