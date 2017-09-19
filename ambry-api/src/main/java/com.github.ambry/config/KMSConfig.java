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
 * Configuration parameters required by a {@link com.github.ambry.router.KeyManagementService}.
 * <p/>
 * Receives the in-memory representation of a properties file and extracts parameters that are specifically
 * required for a {@link com.github.ambry.router.KeyManagementService} and presents them for retrieval through defined APIs.
 */
public class KMSConfig {

  /**
   * The Key gen algorithm that is compatible with the key
   * Ensure this algo goes hand in hand with {@link com.github.ambry.router.CryptoService} implementation
   */
  @Config("kms.key.gen.algo")
  @Default("AES")
  public final String kmsKeyGenAlgo;

  /**
   * The key size that the KMS will populate for random key generations
   */
  @Config("kms.random.key.size.in.bits")
  @Default("256")
  public final int kmsRandomKeySizeInBits;

  public KMSConfig(VerifiableProperties verifiableProperties) {
    kmsKeyGenAlgo = verifiableProperties.getString("kms.key.gen.algo", "AES");
    kmsRandomKeySizeInBits = verifiableProperties.getInt("kms.random.key.size.in.bits", 256);
  }
}
