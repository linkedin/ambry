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
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.KMSConfig;
import java.io.IOException;
import javax.crypto.spec.SecretKeySpec;
import org.bouncycastle.util.encoders.Hex;


/**
 * Default {@link KeyManagementService} which returns a default key {@link SecretKeySpec} for any
 * {@link #getKey(short, short)} calls. Default key is fetched from {@link KMSConfig#kmsDefaultKey}
 */
public class DefaultKeyManagementService implements KeyManagementService<SecretKeySpec> {
  private final String clusterName;
  private final SecretKeySpec secretKeySpec;
  private volatile boolean enabled;

  DefaultKeyManagementService(KMSConfig KMSConfig, ClusterMapConfig clusterMapConfig) {
    if (KMSConfig.kmsDefaultKey.isEmpty()) {
      throw new IllegalArgumentException("Default key cannot be null");
    }
    clusterName = clusterMapConfig.clusterMapClusterName;
    byte[] key = Hex.decode(KMSConfig.kmsDefaultKey);
    this.secretKeySpec = new SecretKeySpec(key, KMSConfig.kmsKeyGenAlgo);
    enabled = true;
  }

  /**
   * Registers with KMS to create key for a unique pair of AccountId and ContainerId
   * @param accountId refers to the id of the {@link Account} to register
   * @param containerId refers to the id of the {@link Container} to register
   * @throws KeyManagementServiceException in registration. {@link DefaultKeyManagementService} doesnt support
   * registration as the default key is read from config.
   */
  @Override
  public void register(short accountId, short containerId) throws KeyManagementServiceException {
    throw new KeyManagementServiceException("Registration is not allowed with DefaultKeyManagementService");
  }

  /**
   * Fetches the key associated with the pair of AccountId and ContainerId. {@link DefaultKeyManagementService} returns
   * the default key for all {@link #getKey(short, short)}} calls fetched from {@link KMSConfig#kmsDefaultKey}
   * @param accountId refers to the id of the {@link Account} for which key is expected
   * @param containerId refers to the id of the {@link Container} for which key is expected
   * @return {@link SecretKeySpec} the key associated with the accountId and containerId
   */
  @Override
  public SecretKeySpec getKey(short accountId, short containerId) throws KeyManagementServiceException {
    if (enabled) {
      return secretKeySpec;
    } else {
      throw new KeyManagementServiceException("getKey() called after DefaultKeyManagementService is closed",
          new IllegalStateException());
    }
  }

  @Override
  public void close() throws IOException {
    enabled = false;
  }
}
