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

/**
 * Crypto Service to assist in encrypting or decrypting bytes
 */
public interface CryptoService<T> {

  /**
   * Encrypts the {@code toEncrypt} using {@link CryptoService}
   * @param toEncrypt bytes to be encrypted
   * @return the byte array containing the encrypted content. Ensure the result has all
   * the information like the IV along with the encrypted content, inorder to decrypt the content with a given key
   * @throws {@link CryptoServiceException} on any exception with encryption
   */
  byte[] encrypt(byte[] toEncrypt) throws CryptoServiceException;

  /**
   * Decrypts the {@code toDecrypt} using {@link CryptoService}
   * @param toDecrypt bytes to be decrypted
   * @return the byte array that will containing the decrypted content
   * @throws {@link CryptoServiceException} on any exception with decryption
   */
  byte[] decrypt(byte[] toDecrypt) throws CryptoServiceException;
}
