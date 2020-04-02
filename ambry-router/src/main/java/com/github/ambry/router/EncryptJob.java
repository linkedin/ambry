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

import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;


/**
 * Class representing an Encrypt Job
 */
public class EncryptJob implements CryptoJob {
  private final short accountId;
  private final short containerId;
  private ByteBuf blobContentToEncrypt;
  private final ByteBuffer userMetadataToEncrypt;
  private final Object perBlobKey;
  private final Callback<EncryptJobResult> callback;
  private final CryptoService cryptoService;
  private final KeyManagementService kms;
  private final CryptoJobMetricsTracker encryptJobMetricsTracker;

  /**
   * Instantiates {@link EncryptJob} for an upload.
   * @param accountId the accountId of the blob that needs to be encrypted
   * @param containerId the containerId of the blob that needs to be encrypted
   * @param blobContentToEncrypt {@link ByteBuf} to be encrypted. Could be {@code null} for a metadata chunk.
   * @param userMetadataToEncrypt user metadata to be encrypted. Could be {@code null} for data chunks.
   * @param perBlobKey per blob key to use to encrypt the blob content
   * @param encryptJobMetricsTracker metrics tracker to track the encryption job
   * @param callback {@link Callback} to be invoked on completion
   */
  EncryptJob(short accountId, short containerId, ByteBuf blobContentToEncrypt, ByteBuffer userMetadataToEncrypt,
      Object perBlobKey, CryptoService cryptoService, KeyManagementService kms,
      CryptoJobMetricsTracker encryptJobMetricsTracker, Callback<EncryptJobResult> callback) {
    this.accountId = accountId;
    this.containerId = containerId;
    this.blobContentToEncrypt = blobContentToEncrypt;
    this.userMetadataToEncrypt = userMetadataToEncrypt;
    this.perBlobKey = perBlobKey;
    this.callback = callback;
    this.cryptoService = cryptoService;
    this.kms = kms;
    this.encryptJobMetricsTracker = encryptJobMetricsTracker;
  }

  /**
   * Steps to be performed on encryption
   * 1. Encrypt blob content using perBlobKey if not null
   * 2. Encrypt user-metadata using perBlobKey if not null
   * 2. Fetch ContainerKey from kms for the given blob
   * 3. Encrypt perBlobKey using containerKey
   * 4. Invoke callback with the encryptedKey and encryptedBlobContent
   */
  @Override
  public void run() {
    encryptJobMetricsTracker.onJobProcessingStart();
    ByteBuffer encryptedUserMetadata = null;
    ByteBuffer encryptedKey = null;
    ByteBuf encryptedBlobContent = null;
    Exception exception = null;
    try {
      if (blobContentToEncrypt != null) {
        encryptedBlobContent = cryptoService.encrypt(blobContentToEncrypt, perBlobKey);
      }
      if (userMetadataToEncrypt != null) {
        encryptedUserMetadata = cryptoService.encrypt(userMetadataToEncrypt, perBlobKey);
      }
      Object containerKey = kms.getKey(accountId, containerId);
      encryptedKey = cryptoService.encryptKey(perBlobKey, containerKey);
    } catch (Exception e) {
      exception = e;
      if (encryptedBlobContent != null) {
        encryptedBlobContent.release();
        encryptedBlobContent = null;
      }
    } finally {
      if (blobContentToEncrypt != null) {
        blobContentToEncrypt.release();
        blobContentToEncrypt = null;
      }
      encryptJobMetricsTracker.onJobProcessingComplete();
      callback.onCompletion(
          exception == null ? new EncryptJobResult(encryptedKey, encryptedUserMetadata, encryptedBlobContent) : null,
          exception);
    }
  }

  /**
   * Close the job with the given {@code gse}
   * @param gse the {@link GeneralSecurityException} that needs to be set while invoking callback for the job
   */
  @Override
  public void closeJob(GeneralSecurityException gse) {
    if (blobContentToEncrypt != null) {
      blobContentToEncrypt.release();
      blobContentToEncrypt = null;
    }
    callback.onCompletion(null, gse);
  }

  /**
   * Class representing encrypt job result
   */
  static class EncryptJobResult {
    private final ByteBuffer encryptedKey;
    private final ByteBuffer encryptedUserMetadata;
    private final ByteBuf encryptedBlobContent;

    EncryptJobResult(ByteBuffer encryptedKey, ByteBuffer encryptedUserMetadata, ByteBuf encryptedBlobContent) {
      this.encryptedKey = encryptedKey;
      this.encryptedUserMetadata = encryptedUserMetadata;
      this.encryptedBlobContent = encryptedBlobContent;
    }

    ByteBuffer getEncryptedKey() {
      return encryptedKey;
    }

    ByteBuf getEncryptedBlobContent() {
      return encryptedBlobContent;
    }

    ByteBuffer getEncryptedUserMetadata() {
      return encryptedUserMetadata;
    }

    void release() {
      if (encryptedBlobContent != null) {
        encryptedBlobContent.release();
      }
    }
  }
}
