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

import com.github.ambry.utils.Utils;
import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;


/**
 * Crypto Service to assist in encrypting and decrypting bytes and keys
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
   * the information like the IV along with the encrypted content, in order to decrypt the content with a given key
   * @throws {@link GeneralSecurityException} on any exception with encryption
   */
  ByteBuffer encrypt(ByteBuffer toEncrypt, T key) throws GeneralSecurityException;

  /**
   * Encrypts the {@code toEncrypt} with the given key. The {@code toEncrypt} is a netty {@link ByteBuf}. This is used
   * for encrypting data chunks.
   * @param toEncrypt {@link ByteBuf} that needs to be encrypted
   * @param key the secret key (of type T) to use to encrypt
   * @return the {@link ByteBuf} containing the encrypted content. Ensure the result has all
   * the information lik the IV along with the encrypted content, in order to decrypt the content with a given key
   * @throws {@link GeneralSecurityException} on any exception with encryption
   */
  default ByteBuf encrypt(ByteBuf toEncrypt, T key) throws GeneralSecurityException {
    return Utils.applyByteBufferFunctionToByteBuf(toEncrypt, buffer -> encrypt(buffer, key));
  }

  /**
   * Decrypts the {@code toDecrypt} with the given key. This is used for decrypting data chunks.
   * @param toDecrypt {@link ByteBuffer} that needs to be decrypted
   * @param key the secret key (of type T) to use to decrypt
   * @return the {@link ByteBuffer} containing the decrypted content
   * @throws {@link GeneralSecurityException} on any exception with decryption
   */
  ByteBuffer decrypt(ByteBuffer toDecrypt, T key) throws GeneralSecurityException;

  /**
   * Decrypts the {@code toDecrypt} with the given key. The {@code toDecrypt} is a netty {@link ByteBuf}. This is used
   * for decrypting data chunks.
   * @param toDecrypt {@link ByteBuf} that needs to be decrypted
   * @param key the secret key (of type T) to use to decrypt
   * @return the {@link ByteBuffer} containing the decrypted content
   * @throws {@link GeneralSecurityException} on any exception with decryption
   */
  default ByteBuf decrypt(ByteBuf toDecrypt, T key) throws GeneralSecurityException {
    return Utils.applyByteBufferFunctionToByteBuf(toDecrypt, buffer -> decrypt(buffer, key));
  }

  /**
   * Returns the encrypted form of the key in bytes.
   * @param  toEncrypt the secret key (of type T) that needs to be encrypted
   * @param  key the secret key (of type T) to use to encrypt
   * @return the {@link ByteBuffer} representing the encrypted key
   * @throws {@link GeneralSecurityException}
   */
  ByteBuffer encryptKey(T toEncrypt, T key) throws GeneralSecurityException;

  /**
   * Decrypts the key using the given {@code key}
   * @param toDecrypt the {@link ByteBuffer} from which key needs to be decrypted
   * @param key  the secret key (of type T) to use to decrypt
   * @return the key thus decrypted
   * @throws {@link GeneralSecurityException}
   */
  T decryptKey(ByteBuffer toDecrypt, T key) throws GeneralSecurityException;
}
