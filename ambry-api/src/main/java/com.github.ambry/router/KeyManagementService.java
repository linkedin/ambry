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


/**
 * Interface that defines the Key management service. KMS is responsible for maintaining keys for every
 * unique triplet of (ClusterName, Account, Container) that is registered with the KMS
 * Every caller is expected to register before making any {@link #getKey(String, Account, Container, Callback)} calls.
 * T refers to the Key type that this {@link KeyManagementService} will generate and return.
 * Ensure that {@link CryptoService} implementation is compatible with the key type that
 * {@link KeyManagementService} generates
 */
public interface KeyManagementService<T> extends Closeable {

  /**
   * Registers with KMS to create key for a unique triplet of (clusterName, Account, Container)
   * @param clusterName the cluster name associated with the account
   * @param account refers to the {@link Account} to register
   * @param container refers to the {@link Container} to register
   */
  void register(String clusterName, Account account, Container container);

  /**
   * Fetches the key associated with the triplet (clusterName, Account, Container). User is expected to have
   * registered using {@link #register(String, Account, Container)} for this triplet before fetching keys.
   * @param clusterName the cluster name associated with the account
   * @param account refers to the {@link Account} to register
   * @param container refers to the {@link Container} to register
   * @param callback the {@link Callback} to be called on completion or on exception.
   */
  void getKey(String clusterName, Account account, Container container, Callback<T> callback);

  /**
   * Fetches the key associated with the triplet (clusterName, Account, Container). User is expected to have
   * registered using {@link #register(String, Account, Container)} for this triplet before fetching keys.
   * @param clusterName the cluster name associated with the account
   * @param account refers to the {@link Account} to register
   * @param container refers to the {@link Container} to register
   * @return the {@link FutureResult} that will containing the key (of type T) on completion or exception
   */
  default FutureResult<T> getKey(String clusterName, Account account, Container container) {
    FutureResult<T> futureResult = new FutureResult<>();
    getKey(clusterName, account, container, futureResult::done);
    return futureResult;
  }
}
