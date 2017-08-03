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

  public CryptoServiceConfig(VerifiableProperties verifiableProperties) {
    cryptoServiceEncryptionDecryptionMode =
        verifiableProperties.getString("crypto.service.encryption.decryption.mode", "GCM");
  }
}
