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
import com.github.ambry.router.KeyManagementService;
import java.nio.ByteBuffer;


/**
 * Configuration parameters required by a {@link CryptoService}.
 * <p/>
 * Receives the in-memory representation of a properties file and extracts parameters that are specifically
 * required for {@link CryptoService} and presents them for retrieval through defined APIs.
 */
public class CryptoServiceConfig {

  /**
   * The mode to encrypt or decrypt. Accepted values are "GCM" for now.
   * Ensure this algo goes hand in hand with {@link KeyManagementService} implementation
   */
  @Config("crypto.service.encryption.decryption.mode")
  @Default("GCM")
  public final String cryptoServiceEncryptionDecryptionMode;

  /**
   * The iv size that the CryptoService will populate on {@link CryptoService#encrypt(ByteBuffer, Object)}
   */
  @Config("crypto.service.iv.size.in.bytes")
  @Default("12")
  public final int cryptoServiceIvSizeInBytes;

  public CryptoServiceConfig(VerifiableProperties verifiableProperties) {
    cryptoServiceEncryptionDecryptionMode =
        verifiableProperties.getString("crypto.service.encryption.decryption.mode", "GCM");
    cryptoServiceIvSizeInBytes = verifiableProperties.getInt("crypto.service.iv.size.in.bytes", 12);
  }
}
