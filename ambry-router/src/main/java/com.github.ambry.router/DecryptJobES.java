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
 * Class representing an decrypt Job.
 */
class DecryptJobES implements CryptoJobES {
  private final BlobId blobId;
  private final ByteBuffer enryptedContent;
  private final ByteBuffer encryptedKey;
  private final Callback<DecryptJobResult> callback;
  private final CryptoService cryptoService;
  private final KeyManagementService kms;

  /**
   * Instantiates {@link DecryptJobES} with {@link BlobId}, key to be decrypted, content to be decrypted and the
   * {@link Callback}
   * @param blobId the {@link BlobId} for which decryption is requested
   * @param encryptedKey encrypted per blob key
   * @param encryptedContent encrypted blob content
   * @param callback {@link Callback} to be invoked on completion
   */
  DecryptJobES(BlobId blobId, ByteBuffer encryptedKey, ByteBuffer encryptedContent,CryptoService cryptoService,
      KeyManagementService kms,
      Callback<DecryptJobResult> callback) {
    this.blobId = blobId;
    this.enryptedContent = encryptedContent;
    this.encryptedKey = encryptedKey;
    this.callback = callback;
    this.cryptoService = cryptoService;
    this.kms = kms;
  }

  /**
   * Steps to be performed on decryption
   * 1. Fetch ContainerKey from kms for the given blob
   * 2. Decrypt encryptedKey using containerKey to obtain perBlobKey
   * 3. Decrypt encryptedContent using perBlobKey
   * 4. Invoke callback with the decryptedContent
   */
  public void run(){
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
}