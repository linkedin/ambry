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

import java.nio.ByteBuffer;


/**
 * Class representing an Encrypt Job
 */
public class EncryptJob extends CryptoJob {
  private final short accountId;
  private final short containerId;
  private final ByteBuffer blobContentToEncrypt;
  private final ByteBuffer userMetadataToEncrypt;
  private final Object perBlobKey;
  private final CryptoService cryptoService;
  private final KeyManagementService kms;
  private final CryptoJobMetricsTracker encryptJobMetricsTracker;

  /**
   * Instantiates {@link EncryptJob} for an upload.
   * @param accountId the accountId of the blob that needs to be encrypted
   * @param containerId the containerId of the blob that needs to be encrypted
   * @param blobContentToEncrypt {@link ByteBuffer} to be encrypted. Could be {@code null} for a metadata chunk.
   * @param userMetadataToEncrypt user metadata to be encrypted. Could be {@code null} for data chunks.
   * @param perBlobKey per blob key to use to encrypt the blob content
   * @param encryptJobMetricsTracker metrics tracker to track the encryption job
   * @param callback {@link Callback} to be invoked on completion
   */
  EncryptJob(short accountId, short containerId, ByteBuffer blobContentToEncrypt, ByteBuffer userMetadataToEncrypt,
      Object perBlobKey, CryptoService cryptoService, KeyManagementService kms,
      CryptoJobMetricsTracker encryptJobMetricsTracker, Callback<EncryptJobResult> callback) {
    super(callback);
    this.accountId = accountId;
    this.containerId = containerId;
    this.blobContentToEncrypt = blobContentToEncrypt;
    this.userMetadataToEncrypt = userMetadataToEncrypt;
    this.perBlobKey = perBlobKey;
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
    if (!isComplete()) {
      encryptJobMetricsTracker.onJobProcessingStart();
      ByteBuffer encryptedBlobContent = null;
      ByteBuffer encryptedUserMetadata = null;
      ByteBuffer encryptedKey = null;
      Exception exception = null;
      if (!isComplete()) {
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
        } finally {
          encryptJobMetricsTracker.onJobProcessingComplete();
        }
        completeJob(
            exception == null ? new EncryptJobResult(encryptedKey, encryptedBlobContent, encryptedUserMetadata) : null,
            exception);
      }
    }
  }

  /**
   * Class representing encrypt job result
   */
  class EncryptJobResult {
    private final ByteBuffer encryptedKey;
    private final ByteBuffer encryptedBlobContent;
    private final ByteBuffer encryptedUserMetadata;

    EncryptJobResult(ByteBuffer encryptedKey, ByteBuffer encryptedBlobContent, ByteBuffer encryptedUserMetadata) {
      this.encryptedKey = encryptedKey;
      this.encryptedBlobContent = encryptedBlobContent;
      this.encryptedUserMetadata = encryptedUserMetadata;
    }

    ByteBuffer getEncryptedKey() {
      return encryptedKey;
    }

    ByteBuffer getEncryptedBlobContent() {
      return encryptedBlobContent;
    }

    ByteBuffer getEncryptedUserMetadata() {
      return encryptedUserMetadata;
    }
  }
}
