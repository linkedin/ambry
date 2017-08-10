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

import com.github.ambry.config.CryptoServiceConfig;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.security.Security;
import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Default {@link CryptoService} which is capable of encrypting or decrypting bytes based on the given key.
 * As of now supports GCM only for enryption and decryption
 */
class DefaultCryptoService implements CryptoService<SecretKeySpec> {

  private static final String GCM_CRYPTO_INSTANCE = "AES/GCM/NoPadding";
  private static final SecureRandom random = new SecureRandom();
  private static final int ivValSize = 12;
  private final Mode mode;
  private final CryptoServiceConfig config;

  private static final Logger logger = LoggerFactory.getLogger(DefaultCryptoService.class);

  DefaultCryptoService(CryptoServiceConfig cryptoServiceConfig) {
    this.config = cryptoServiceConfig;
    Security.addProvider(new BouncyCastleProvider());
    switch (config.cryptoServiceEncryptionDecryptionMode) {
      case "GCM":
        mode = Mode.GCM;
        break;
      default:
        throw new IllegalArgumentException(
            "Unrecognized Encryption Decryption Mode " + config.cryptoServiceEncryptionDecryptionMode);
    }
  }

  @Override
  public ByteBuffer encrypt(ByteBuffer toEncrypt, SecretKeySpec key) throws CryptoServiceException {
    try {
      Cipher encrypter = Cipher.getInstance(GCM_CRYPTO_INSTANCE, "BC");
      byte[] iv = new byte[ivValSize];
      random.nextBytes(iv);
      encrypter.init(Cipher.ENCRYPT_MODE, key, new IvParameterSpec(iv));
      int outputSize = encrypter.getOutputSize(toEncrypt.remaining());
      ByteBuffer encryptedContent = ByteBuffer.allocate(iv.length + outputSize);
      encryptedContent.put(iv);
      encrypter.doFinal(toEncrypt, encryptedContent);
      encryptedContent.flip();
      return encryptedContent;
    } catch (Exception e) {
      throw new CryptoServiceException("Exception thrown while encrypting data", e);
    }
  }

  @Override
  public ByteBuffer decrypt(ByteBuffer toDecrypt, SecretKeySpec key) throws CryptoServiceException {
    try {
      Cipher decrypter = Cipher.getInstance(GCM_CRYPTO_INSTANCE, "BC");
      byte[] iv = new byte[ivValSize];
      toDecrypt.get(iv);
      decrypter.init(Cipher.DECRYPT_MODE, key, new IvParameterSpec(iv));
      ByteBuffer decryptedContent = ByteBuffer.allocate(decrypter.getOutputSize(toDecrypt.remaining()));
      decrypter.doFinal(toDecrypt, decryptedContent);
      decryptedContent.flip();
      return decryptedContent;
    } catch (Exception e) {
      throw new CryptoServiceException("Exception thrown while decrypting data", e);
    }
  }

  /**
   * Supported Modes of encryption/decryption
   */
  enum Mode {
    GCM;
  }
}
