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
package com.github.ambry.config;

import com.github.ambry.router.CryptoService;
import java.nio.ByteBuffer;


/**
 * Configuration parameters required by a {@link com.github.ambry.router.CryptoService}.
 * <p/>
 * Receives the in-memory representation of a properties file and extracts parameters that are specifically
 * required for {@link com.github.ambry.router.CryptoService} and presents them for retrieval through defined APIs.
 */
public class CryptoServiceConfig {

  /**
   * The mode to encrypt or decrypt. Accepted values are "GCM" for now.
   * Ensure this algo goes hand in hand with {@link com.github.ambry.router.KeyManagementService} implementation
   */
  @Config("crypto.service.encryption.decryption.mode")
  @Default("GCM")
  public final String cryptoServiceEncryptionDecryptionMode;

  /**
   * The iv size that the CryptoService will populate on {@link com.github.ambry.router.CryptoService#encrypt(ByteBuffer, Object)}
   */
  @Config("crypto.service.iv.size.in.bytes")
  @Default("12")
  public final int cryptoServiceIvSizeInBytes;

  /**
   * The Key gen algorithm for the key that {@link CryptoService#getRandomKey()} generates
   */
  @Config("crypto.service.key.gen.algo")
  @Default("AES")
  public final String cryptoServiceKeyGenAlgo;

  /**
   * The random key size in chars that the CryptoService will populate on {@link CryptoService#getRandomKey()}
   */
  @Config("crypto.service.key.size.in.chars")
  @Default("64")
  public final int cryptoServiceKeySizeInChars;

  public CryptoServiceConfig(VerifiableProperties verifiableProperties) {
    cryptoServiceEncryptionDecryptionMode =
        verifiableProperties.getString("crypto.service.encryption.decryption.mode", "GCM");
    cryptoServiceIvSizeInBytes = verifiableProperties.getInt("crypto.service.iv.size.in.bytes", 12);
    cryptoServiceKeySizeInChars = verifiableProperties.getInt("crypto.service.key.size.in.chars", 64);
    cryptoServiceKeyGenAlgo = verifiableProperties.getString("crypto.service.key.gen.algo", "AES");
  }
}
