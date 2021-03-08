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
package com.github.ambry.frontend;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.accountstats.AccountStatsStore;
import com.github.ambry.accountstats.AccountStatsStoreFactory;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.StatsManagerConfig;
import com.github.ambry.config.VerifiableProperties;

import static org.mockito.Mockito.*;


/**
 * Factory implementation to return a mock {@link AccountStatsStore}.
 */
public class MockAccountStatsStoreFactory implements AccountStatsStoreFactory {
  private final AccountStatsStore accountStatsStore;

  public MockAccountStatsStoreFactory(VerifiableProperties verifiableProperties, ClusterMapConfig clusterMapConfig,
      StatsManagerConfig statsManagerConfig, MetricRegistry registry) {
    accountStatsStore = mock(AccountStatsStore.class);
  }

  @Override
  public AccountStatsStore getAccountStatsStore() throws Exception {
    return accountStatsStore;
  }
}
