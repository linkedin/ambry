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
import java.util.concurrent.atomic.AtomicBoolean;
import javax.crypto.spec.SecretKeySpec;
import org.bouncycastle.util.encoders.Hex;


/**
 * Default {@link KeyManagementService} which returns a default key {@link SecretKeySpec} for any
 * {@link #getKey(String, Account, Container, Callback)} calls. Default key is fetched from {@link KMSConfig#kmsDefaultKey}
 */
public class DefaultKeyManagementService implements KeyManagementService<SecretKeySpec> {
  private final SecretKeySpec secretKeySpec;
  private final AtomicBoolean enabled;

  DefaultKeyManagementService(KMSConfig KMSConfig) {
    if (KMSConfig.kmsDefaultKey.isEmpty()) {
      throw new IllegalArgumentException("Default key cannot be null");
    }
    byte[] key = Hex.decode(KMSConfig.kmsDefaultKey);
    this.secretKeySpec = new SecretKeySpec(key, KMSConfig.kmsKeyGenAlgo);
    enabled = new AtomicBoolean(true);
  }

  /**
   * Registers with KMS to create key for a unique triplet of (clusterName, Account, Container)
   * @param clusterName the cluster name associated with the account
   * @param account refers to the {@link Account} to register
   * @param container refers to the {@link Container} to register
   */
  @Override
  public void register(String clusterName, Account account, Container container) {
    // no op
  }

  /**
   * Fetches the key associated with the triplet (clusterName, Account, Container). {@link DefaultKeyManagementService} returns
   * the default key for all {@link #getKey(String, Account, Container, Callback)} calls fetched from {@link KMSConfig#kmsDefaultKey}
   * @param clusterName the cluster name associated with the account
   * @param account refers to the {@link Account} to register
   * @param container refers to the {@link Container} to register
   * @param callback the {@link Callback} to be called on completion or on exception
   */
  @Override
  public void getKey(String clusterName, Account account, Container container, Callback<SecretKeySpec> callback) {
    if (enabled.get()) {
      callback.onCompletion(secretKeySpec, null);
    } else {
      throw new IllegalStateException("GetKey() called after DefaultKeyManagementService is closed");
    }
  }

  @Override
  public void close() throws IOException {
    enabled.compareAndSet(true, false);
  }
}
