/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud;

import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;


/**
 * Crypto Service to assist in encrypting and decrypting blob chunks transferred between
 * Ambry and the cloud backup.
 */
public interface CloudBlobCryptoAgent {

  /**
   * Encrypts the blob buffer being uploaded to cloud storage.
   * @param buffer The {@link ByteBuffer} that needs to be encrypted.
   * @return The encrypted buffer.
   * @throws {@link GeneralSecurityException} on any exception with encryption.
   */
  ByteBuffer encrypt(ByteBuffer buffer) throws GeneralSecurityException;

  /**
   * Decrypts the blob buffer being downloaded from cloud storage.
   * @param buffer The {@link ByteBuffer} that needs to be decrypted.
   * @return The decrypted buffer.
   * @throws {@link GeneralSecurityException} on any exception with decryption
   */
  ByteBuffer decrypt(ByteBuffer buffer) throws GeneralSecurityException;

  /**
   * Returns the encryption context (if any)
   * @return
   */
  String getEncryptionContext();
}
