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

import com.github.ambry.account.Account;
import com.github.ambry.account.Container;
import com.github.ambry.config.KMSConfig;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.bouncycastle.util.encoders.Hex;


/**
 * Single {@link KeyManagementService} which returns a default key {@link SecretKeySpec} for any
 * {@link #getKey(short, short)} calls.
 */
public class SingleKeyManagementService implements KeyManagementService<SecretKeySpec> {
  private final SecretKeySpec secretKeySpec;
  private final KMSConfig config;
  private volatile boolean enabled = true;

  SingleKeyManagementService(KMSConfig config, String defaultKey) {
    byte[] key = Hex.decode(defaultKey);
    secretKeySpec = new SecretKeySpec(key, config.kmsKeyGenAlgo);
    this.config = config;
  }

  /**
   * Registers with KMS to create key for a unique pair of AccountId and ContainerId
   * @param accountId refers to the id of the {@link Account} to register
   * @param containerId refers to the id of the {@link Container} to register
   */
  @Override
  public void register(short accountId, short containerId) {
    // no op
  }

  /**
   * Fetches the key associated with the pair of AccountId and ContainerId. {@link SingleKeyManagementService} returns
   * the default key for all {@link #getKey(short, short)}}
   * @param accountId refers to the id of the {@link Account} for which key is expected
   * @param containerId refers to the id of the {@link Container} for which key is expected
   * @return {@link SecretKeySpec} the key associated with the accountId and containerId
   */
  @Override
  public SecretKeySpec getKey(short accountId, short containerId) throws GeneralSecurityException {
    if (enabled) {
      return secretKeySpec;
    } else {
      throw new GeneralSecurityException("getKey() called after SingleKeyManagementService is closed");
    }
  }

  @Override
  public SecretKeySpec getRandomKey() throws GeneralSecurityException {
    KeyGenerator keyGen = null;
    try {
      keyGen = KeyGenerator.getInstance("AES");
      keyGen.init(config.kmsRandomKeySizeInBits);
      SecretKey secretKey = keyGen.generateKey();
      byte[] encoded = secretKey.getEncoded();
      return new SecretKeySpec(encoded, "AES");
    } catch (NoSuchAlgorithmException e) {
      throw new GeneralSecurityException("NoSuchAlgorithmException thrown while generating random key", e);
    }
  }

  @Override
  public void close() throws IOException {
    enabled = false;
  }
}
