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
 * Crypto Service to assist in encrypting or decrypting bytes
 * T refers to the key type that this {@link CryptoService} accepts.
 * Ensure that {@link CryptoService} implementation is compatible with the key type that
 * {@link KeyManagementService} generates
 */
public interface CryptoService<T> {

  /**
   * Encrypts the {@code toEncrypt} with the given key
   * @param toEncrypt {@link ByteBuffer} that needs to be encrypted
   * @param key the secret key (of type T) to use to encrypt
   * @return the {@link ByteBuffer} containing the encrypted content. Ensure the result has all
   * the information like the IV along with the encrypted content, inorder to decrypt the content with a given key
   * @throws {@link CryptoServiceException} on any exception with encryption
   */
  ByteBuffer encrypt(ByteBuffer toEncrypt, T key) throws CryptoServiceException;

  /**
   * Decrypts the {@code toDecrypt} with the given key
   * @param toDecrypt {@link ByteBuffer} that needs to be decrypted
   * @param key the secret key (of type T) to use to decrypt
   * @return the {@link ByteBuffer} that will containing the decrypted content
   * @throws {@link CryptoServiceException} on any exception with decryption
   */
  ByteBuffer decrypt(ByteBuffer toDecrypt, T key) throws CryptoServiceException;
}
