/**
 * Copyright 2021 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.accountstats;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.VerifiableProperties;


/**
 * Factory for {@link InmemoryAccountStatsStore}.
 */
public class InmemoryAccountStatsStoreFactory implements AccountStatsStoreFactory {
  private final AccountStatsStore accountStatsStore;

  /**
   * Constructor to create a {@link InmemoryAccountStatsStoreFactory}.
   * @param verifiableProperties
   * @param clusterMapConfig
   * @param registry
   */
  public InmemoryAccountStatsStoreFactory(VerifiableProperties verifiableProperties, ClusterMapConfig clusterMapConfig,
      MetricRegistry registry) {
    accountStatsStore =
        new InmemoryAccountStatsStore(clusterMapConfig.clusterMapClusterName, clusterMapConfig.clusterMapHostName);
  }

  @Override
  public AccountStatsStore getAccountStatsStore() throws Exception {
    return accountStatsStore;
  }
}
