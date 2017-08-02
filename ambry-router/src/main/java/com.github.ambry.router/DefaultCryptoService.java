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
import java.security.Security;
import java.util.Random;
import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Default {@link CryptoService} which is capable of encrypting or decrypting bytes based on the given key.
 */
class DefaultCryptoService implements CryptoService<SecretKeySpec> {

  private static final Random random = new Random();
  private static final int ivValSize = 12; // 96 bits
  private final SecretKeySpec key;
  private final CryptoServiceConfig config;

  private static final Logger logger = LoggerFactory.getLogger(DefaultCryptoService.class);

  DefaultCryptoService(CryptoServiceConfig cryptoServiceConfig, SecretKeySpec key) {
    this.key = key;
    this.config = cryptoServiceConfig;
    Security.addProvider(new BouncyCastleProvider());
  }

  @Override
  public byte[] encrypt(byte[] toEncrypt) throws CryptoServiceException {
    try {
      Cipher encrypter = Cipher.getInstance(config.cryptoServiceEncryptionDecryptionAlgo, "BC");
      byte[] iv = new byte[ivValSize];
      random.nextBytes(iv);
      encrypter.init(Cipher.ENCRYPT_MODE, key, new IvParameterSpec(iv));
      int outputSize = encrypter.getOutputSize(toEncrypt.length);
      ByteBuffer input = ByteBuffer.wrap(toEncrypt);
      ByteBuffer output = ByteBuffer.allocate(ivValSize + outputSize);
      output.put(iv);
      encrypter.doFinal(input, output);
      return output.array();
    } catch (Exception e) {
      logger.error("Exception thrown while encryting data", e);
      throw new CryptoServiceException("Exception thrown while encryting data", e);
    }
  }

  @Override
  public byte[] decrypt(byte[] toDecrypt) throws CryptoServiceException {
    try {
      Cipher decrypter = Cipher.getInstance(config.cryptoServiceEncryptionDecryptionAlgo, "BC");
      ByteBuffer input = ByteBuffer.wrap(toDecrypt);
      byte[] iv = new byte[ivValSize];
      input.get(iv);
      decrypter.init(Cipher.DECRYPT_MODE, key, new IvParameterSpec(iv));
      ByteBuffer output = ByteBuffer.allocate(decrypter.getOutputSize(input.remaining()));
      decrypter.doFinal(input, output);
      return output.array();
    } catch (Exception e) {
      logger.error("Exception thrown while decrypting data", e);
      throw new CryptoServiceException("Exception thrown while decrypting data", e);
    }
  }
}
