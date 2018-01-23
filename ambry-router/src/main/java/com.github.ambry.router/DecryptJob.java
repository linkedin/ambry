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


/**
 * Class representing an decrypt Job.
 */
class DecryptJob extends CryptoJob {
  private final BlobId blobId;
  private final ByteBuffer encryptedBlobContent;
  private final ByteBuffer encryptedUserMetadata;
  private final ByteBuffer encryptedPerBlobKey;
  private final CryptoService cryptoService;
  private final KeyManagementService kms;
  private final CryptoJobMetricsTracker decryptJobMetricsTracker;

  /**
   * Instantiates {@link DecryptJob} with {@link BlobId}, key to be decrypted, content to be decrypted and the
   * {@link Callback}
   * @param blobId the {@link BlobId} for which decryption is requested
   * @param encryptedPerBlobKey encrypted per blob key
   * @param encryptedBlobContent encrypted blob content. Could be {@null}
   * @param encryptedUserMetadata encrypted user metadata. Could be {@null}
   * @param cryptoService the {@link CryptoService} instance to use
   * @param kms the {@link KeyManagementService} instance to use
   * @param decryptJobMetricsTracker metrics tracker to track the decryption job
   * @param callback {@link Callback} to be invoked on completion
   */
  DecryptJob(BlobId blobId, ByteBuffer encryptedPerBlobKey, ByteBuffer encryptedBlobContent,
      ByteBuffer encryptedUserMetadata, CryptoService cryptoService, KeyManagementService kms,
      CryptoJobMetricsTracker decryptJobMetricsTracker, Callback<DecryptJobResult> callback) {
    super(callback);
    this.blobId = blobId;
    this.encryptedBlobContent = encryptedBlobContent;
    this.encryptedUserMetadata = encryptedUserMetadata;
    this.encryptedPerBlobKey = encryptedPerBlobKey;
    this.cryptoService = cryptoService;
    this.kms = kms;
    this.decryptJobMetricsTracker = decryptJobMetricsTracker;
  }

  /**
   * Steps to be performed on decryption
   * 1. Fetch ContainerKey from kms for the given blob
   * 2. Decrypt encryptedPerBlobKey using containerKey to obtain perBlobKey
   * 3. Decrypt encryptedContent using perBlobKey if not null
   * 4. Decrypt encryptedUserMeta using perBlobKey if not null
   * 5. Invoke callback with the decryptedBlobContent
   */
  public void run() {
    decryptJobMetricsTracker.onJobProcessingStart();
    Exception exception = null;
    ByteBuffer decryptedBlobContent = null;
    ByteBuffer decryptedUserMetadata = null;
    if (!isComplete()) {
      try {
        Object containerKey = kms.getKey(blobId.getAccountId(), blobId.getContainerId());
        Object perBlobKey = cryptoService.decryptKey(encryptedPerBlobKey, containerKey);
        if (encryptedBlobContent != null) {
          decryptedBlobContent = cryptoService.decrypt(encryptedBlobContent, perBlobKey);
        }
        if (encryptedUserMetadata != null) {
          decryptedUserMetadata = cryptoService.decrypt(encryptedUserMetadata, perBlobKey);
        }
      } catch (Exception e) {
        exception = e;
      } finally {
        decryptJobMetricsTracker.onJobProcessingComplete();
        completeJob(
            exception == null ? new DecryptJobResult(blobId, decryptedBlobContent, decryptedUserMetadata) : null,
            exception);
      }
    }
  }

  /**
   * Class respresenting decrypt job result
   */
  class DecryptJobResult {
    private final BlobId blobId;
    private final ByteBuffer decryptedBlobContent;
    private final ByteBuffer decryptedUserMetadata;

    DecryptJobResult(BlobId blobId, ByteBuffer decryptedBlobContent, ByteBuffer decryptedUserMetadata) {
      this.blobId = blobId;
      this.decryptedBlobContent = decryptedBlobContent;
      this.decryptedUserMetadata = decryptedUserMetadata;
    }

    BlobId getBlobId() {
      return blobId;
    }

    ByteBuffer getDecryptedBlobContent() {
      return decryptedBlobContent;
    }

    ByteBuffer getDecryptedUserMetadata() {
      return decryptedUserMetadata;
    }
  }
}
