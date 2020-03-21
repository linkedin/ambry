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
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.bouncycastle.util.encoders.Hex;


/**
 * Single {@link KeyManagementService} which returns a default key {@link SecretKeySpec} for any
 * {@link #getKey(short, short)} calls, but a different random key for any {@link #getRandomKey()} calls.
 */
public class SingleKeyManagementService implements KeyManagementService<SecretKeySpec> {
  private final SecretKeySpec secretKeySpec;
  private volatile boolean enabled = true;
  private final KeyGenerator keyGen;
  private final String keyGenAlgo;

  /**
   * Instantiates {@link SingleKeyManagementService} with {@link KMSConfig} and a default container key which is hex string.
   * @param config {@link KMSConfig} to fetch configs from
   * @param defaultContainerKey the default container key that will be returned for all {@link #getKey(short, short)} calls
   * @throws GeneralSecurityException
   */
  public SingleKeyManagementService(KMSConfig config, String defaultContainerKey) throws GeneralSecurityException {
    byte[] key = Hex.decode(defaultContainerKey);
    keyGenAlgo = config.kmsKeyGenAlgo;
    secretKeySpec = new SecretKeySpec(key, keyGenAlgo);
    keyGen = KeyGenerator.getInstance(keyGenAlgo);
    keyGen.init(config.kmsRandomKeySizeInBits);
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

  @Override
  public void register(String context) throws GeneralSecurityException {
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
    return getKey(accountId + ":" + containerId);
  }

  @Override
  public SecretKeySpec getKey(String context) throws GeneralSecurityException {
    if (enabled) {
      return secretKeySpec;
    } else {
      throw new GeneralSecurityException("getKey() called after SingleKeyManagementService is closed");
    }
  }

  @Override
  public SecretKeySpec getRandomKey() throws GeneralSecurityException {
    SecretKey secretKey = keyGen.generateKey();
    byte[] encoded = secretKey.getEncoded();
    return new SecretKeySpec(encoded, keyGenAlgo);
  }

  @Override
  public void close() throws IOException {
    enabled = false;
  }
}
