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
import java.io.Closeable;
import java.security.GeneralSecurityException;


/**
 * Interface that defines the Key management service. KMS is responsible for maintaining keys for every
 * unique pair of AccountId and ContainerId that is registered with the KMS
 * Every caller is expected to register before making any {@link #getKey(short, short)} calls.
 * T refers to the Key type that this {@link KeyManagementService} will generate and return.
 * Ensure that {@link CryptoService} implementation is compatible with the key type that
 * {@link KeyManagementService} generates
 */
public interface KeyManagementService<T> extends Closeable {

  /**
   * Registers with KMS to create key for a unique pair of AccountId and ContainerId
   * @param accountId refers to the id of the {@link Account} to register
   * @param containerId refers to the id of the {@link Container} to register
   * @throws {@link GeneralSecurityException} on KMS unavailability or duplicate registration
   */
  void register(short accountId, short containerId) throws GeneralSecurityException;

  /**
   * Registers with KMS to create key for a unique context.
   * @param context refers to the key context to register
   * @throws {@link GeneralSecurityException} on KMS unavailability or duplicate registration
   */
  void register(String context) throws GeneralSecurityException;

  /**
   * Fetches the key associated with the pair of AccountId and ContainerId. User is expected to have
   * registered using {@link #register(short, short)} for this pair before fetching keys.
   * @param accountId refers to the id of the {@link Account} for which key is expected
   * @param containerId refers to the id of the {@link Container} for which key is expected
   * @return T the key associated with the accountId and containerId
   * @throws {@link GeneralSecurityException} on KMS unavailability or if key is not registered
   */
  T getKey(short accountId, short containerId) throws GeneralSecurityException;

  /**
   * Fetches the key associated with the specified context. User is expected to have
   * registered using {@link #register(String)} for this context before fetching keys.
   * @param context refers to the context for which key is expected
   * @return T the key associated with the context
   * @throws {@link GeneralSecurityException} on KMS unavailability or if key is not registered
   */
  T getKey(String context) throws GeneralSecurityException;

  /**
   * Generate and return a random key (of type T)
   * @return a random key (of type T)
   */
  T getRandomKey() throws GeneralSecurityException;
}
