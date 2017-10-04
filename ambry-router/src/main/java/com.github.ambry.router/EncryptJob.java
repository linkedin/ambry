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


/**
 * Class representing an Encrypt Job
 */
public class EncryptJob implements CryptoJob {
  private final BlobId blobId;
  private final ByteBuffer toEncrypt;
  private final Object perBlobKey;
  private final Callback<EncryptJobResult> callback;
  private final CryptoService cryptoService;
  private final KeyManagementService kms;

  /**
   * Instantiates {@link EncryptJob} with {@link BlobId}, content to encrypt, perBlobKey and the {@link Callback}
   * @param blobId the {@link BlobId} for which encryption is requested
   * @param toEncrypt {@link ByteBuffer} to be encrypted
   * @param perBlobKey per blob key to use to encrypt the blob content
   * @param callback {@link Callback} to be invoked on completion
   */
  EncryptJob(BlobId blobId, ByteBuffer toEncrypt, Object perBlobKey, CryptoService cryptoService,
      KeyManagementService kms, Callback<EncryptJobResult> callback) {
    this.blobId = blobId;
    this.toEncrypt = toEncrypt;
    this.perBlobKey = perBlobKey;
    this.callback = callback;
    this.cryptoService = cryptoService;
    this.kms = kms;
  }

  /**
   * Steps to be performed on encryption
   * 1. Encrypt blob content using perBlobKey
   * 2. Fetch ContainerKey from kms for the given blob
   * 3. Encrypt perBlobKey using containerKey
   * 4. Invoke callback with the encryptedKey and encryptedContent
   */
  @Override
  public void run() {
    try {
      ByteBuffer encryptedContent = cryptoService.encrypt(toEncrypt, perBlobKey);
      Object containerKey = kms.getKey(blobId.getAccountId(), blobId.getContainerId());
      ByteBuffer encryptedKey = cryptoService.encryptKey(perBlobKey, containerKey);
      callback.onCompletion(new EncryptJobResult(blobId, encryptedKey, encryptedContent), null);
    } catch (Exception e) {
      callback.onCompletion(new EncryptJobResult(blobId, null, null), e);
    }
  }

  /**
   * Close the job with the given {@code gse}
   * @param gse the {@link GeneralSecurityException} that needs to be set while invoking callback for the job
   */
  public void closeJob(GeneralSecurityException gse) {
    callback.onCompletion(new EncryptJobResult(blobId, null, null), gse);
  }

  /**
   * Class representing encrypt job result
   */
  class EncryptJobResult {
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
}
