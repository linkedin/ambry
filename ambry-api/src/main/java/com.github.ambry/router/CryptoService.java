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
   * Encrypts the {@code toEncrypt} with the given key. This is used for encrypting data chunks.
   * @param toEncrypt {@link ByteBuffer} that needs to be encrypted
   * @param key the secret key (of type T) to use to encrypt
   * @return the {@link ByteBuffer} containing the encrypted content. Ensure the result has all
   * the information like the IV along with the encrypted content, inorder to decrypt the content with a given key
   * @throws {@link CryptoServiceException} on any exception with encryption
   */
  ByteBuffer encrypt(ByteBuffer toEncrypt, T key) throws CryptoServiceException;

  /**
   * Decrypts the {@code toDecrypt} with the given key. This is used for decrypting data chunks.
   * @param toDecrypt {@link ByteBuffer} that needs to be decrypted
   * @param key the secret key (of type T) to use to decrypt
   * @return the {@link ByteBuffer} containing the decrypted content
   * @throws {@link CryptoServiceException} on any exception with decryption
   */
  ByteBuffer decrypt(ByteBuffer toDecrypt, T key) throws CryptoServiceException;

  /**
   * Encrypts the {@code toEncrypt} with the given key. This is used for encrypting keys(of type T)
   * @param toEncrypt the byte array that needs to be encrypted
   * @param key the secret key (of type T) to use to encrypt
   * @return the byte array containing the encrypted content. Ensure the result has all
   * the information like the IV along with the encrypted content, inorder to decrypt the content with a given key
   * @throws {@link CryptoServiceException} on any exception with encryption
   */
  byte[] encrypt(byte[] toEncrypt, T key) throws CryptoServiceException;

  /**
   * Decrypts the {@code toDecrypt} with the given key. This is used for decrypting keys (of type T)
   * @param toDecrypt the byte array that needs to be decrypted
   * @param key the secret key (of type T) to use to decrypt
   * @return the byte array containing the decrypted content
   * @throws {@link CryptoServiceException} on any exception with decryption
   */
  byte[] decrypt(byte[] toDecrypt, T key) throws CryptoServiceException;

  /**
   * Generate and return a random key (of type T)
   * @return a random key (of type T)
   */
  T getRandomKey();

  /**
   * Returns the bytes to serialize for a given key. This does not necessarily mean entire key object has to converted
   * to bytes. Only requirement is that, same key should be constructible when the same bytes is passed into
   * {@link #getBytesToSerializeForKey(Object)}
   * @param  key the secret key (of type T) to for which bytes are requested
   * @return the bytes to serialize for the given key
   */
  byte[] getBytesToSerializeForKey(T key);

  /**
   * Constructs a key from the given bytes. Implementors should ensure that for any given key, calling
   * {@link #getBytesToSerializeForKey(Object)} followed by {@link #getKeyFromBytes(byte[])} should
   * fetch the same key
   * @param bytes byte array from which key needs to be constructed
   * @return the key thus constructed from the bytes passed in
   */
  T getKeyFromBytes(byte[] bytes);

  /**
   * Returns the encrypted form of the key in bytes.
   * {@link #getBytesToSerializeForKey(Object)}
   * @param  keyToBeEncrypted the secret key (of type T) that needs to be encrypted
   * @param  keyToEncrypt the secret key (of type T) to use to encrypt
   * @return the bytes representing the encrypted key
   */
  byte[] encryptKey(T keyToBeEncrypted, T keyToEncrypt);

  /**
   * Decrypts the key using the given {@code keyToDecrypt}
   * @param bytes byte array from which key needs to be decrypted
   * @param keyToDecrypt  the secret key (of type T) to use to decrypt
   * @return the key thus decrypted
   */
  T decryptKey(byte[] bytes, T keyToDecrypt);
}
