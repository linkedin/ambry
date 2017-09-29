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
 * CryptoJob representing the job that needs processing by the {@link CryptoWorker}
 */
public interface CryptoJob {
  /**
   * Processing of the job using {@code cryptoService} and {@code kms}.
   * @param cryptoService the {@link CryptoService} to assist in encrypting and decrypting
   * @param kms the {@link KeyManagementService} to fetch keys from
   */
  void doOperation(CryptoService cryptoService, KeyManagementService kms);

  /**
   * Close the job with the given {@code gse}
   * @param gse the {@link GeneralSecurityException} that needs to be set while invoking callback for the job
   */
  void closeJob(GeneralSecurityException gse);

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

  /**
   * Class respresenting decrypt job result
   */
  class DecryptJobResult {
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
  class EncryptJob implements CryptoJob {
    private BlobId blobId;
    private ByteBuffer toEncrypt;
    private final Object perBlobKey;
    private final Callback<EncryptJobResult> callback;

    /**
     * Instantiates {@link EncryptJob} with {@link BlobId}, content to encrypt, perBlobKey and the {@link Callback}
     * @param blobId the {@link BlobId} for which encryption is requested
     * @param toEncrypt {@link ByteBuffer} to be encrypted
     * @param perBlobKey per blob key to use to encrypt the blob content
     * @param callback {@link Callback} to be invoked on completion
     */
    EncryptJob(BlobId blobId, ByteBuffer toEncrypt, Object perBlobKey, Callback<EncryptJobResult> callback) {
      this.blobId = blobId;
      this.toEncrypt = toEncrypt;
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
    public void doOperation(CryptoService cryptoService, KeyManagementService kms) {
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
  }

  /**
   * Class representing an decrypt Job.
   */
  class DecryptJob implements CryptoJob {
    private final BlobId blobId;
    private final ByteBuffer enryptedContent;
    private final ByteBuffer encryptedKey;
    private final Callback<DecryptJobResult> callback;

    /**
     * Instantiates {@link DecryptJob} with {@link BlobId}, key to be decrypted, content to be decrypted and the
     * {@link Callback}
     * @param blobId the {@link BlobId} for which decryption is requested
     * @param encryptedKey encrypted per blob key
     * @param encryptedContent encrypted blob content
     * @param callback {@link Callback} to be invoked on completion
     */
    DecryptJob(BlobId blobId, ByteBuffer encryptedKey, ByteBuffer encryptedContent,
        Callback<DecryptJobResult> callback) {
      this.blobId = blobId;
      this.enryptedContent = encryptedContent;
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
    public void doOperation(CryptoService cryptoService, KeyManagementService kms) {
      try {
        Object containerKey = kms.getKey(blobId.getAccountId(), blobId.getContainerId());
        Object perBlobKey = cryptoService.decryptKey(encryptedKey, containerKey);
        ByteBuffer decryptedContent = cryptoService.decrypt(enryptedContent, perBlobKey);
        callback.onCompletion(new DecryptJobResult(blobId, decryptedContent), null);
      } catch (Exception e) {
        callback.onCompletion(new DecryptJobResult(blobId, null), e);
      }
    }

    /**
     * Close the job with the given {@code gse}
     * @param gse the {@link GeneralSecurityException} that needs to be set while invoking callback for the job
     */
    public void closeJob(GeneralSecurityException gse) {
      callback.onCompletion(new DecryptJobResult(blobId, null), gse);
    }
  }
}
