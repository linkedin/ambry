/**
 * Copyright 2023 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.AccountService;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.network.NetworkClient;
import com.github.ambry.network.NetworkClientFactory;
import com.github.ambry.replication.FindTokenHelper;
import com.github.ambry.server.StoreManager;
import java.io.IOException;


/**
 * Factory to create a {@link RecoveryNetworkClient}.
 */
public class RecoveryNetworkClientFactory implements NetworkClientFactory {
  private final ClusterMap clustermap;
  private final StoreManager storeManager;
  private final VerifiableProperties properties;
  private final MetricRegistry registry;
  private final AccountService accountService;

  /**
   * Constructor to create the factory
   *
   * @param registry
   * @param clusterMap      The {@link ClusterMap} object.
   * @param storeManager    The {@link StoreManager} object.
   * @param accountService
   */
  public RecoveryNetworkClientFactory(VerifiableProperties properties, MetricRegistry registry, ClusterMap clusterMap,
      StoreManager storeManager, AccountService accountService) {
    this.properties = properties;
    this.registry = registry;
    this.clustermap = clusterMap;
    this.storeManager = storeManager;
    this.accountService = accountService;
  }

  @Override
  public NetworkClient getNetworkClient() throws IOException {
    return new RecoveryNetworkClient(properties, registry, clustermap, storeManager, accountService);
  }
}
