/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.quota.storage;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.StorageQuotaConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.accountstats.AccountStatsStore;


/**
 * A factory implementation for {@link StorageQuotaService}.
 */
public class AmbryStorageQuotaServiceFactory implements StorageQuotaServiceFactory {
  private final StorageQuotaService storageQuotaService;

  /**
   * Constructor to create {@link AmbryStorageQuotaServiceFactory}.
   * @param config The {@link StorageQuotaConfig}.
   * @param accountStatsStore The {@link AccountStatsStore}.
   * @param metricRegistry The {@link MetricRegistry} to register new metrics.
   * @throws Exception
   */
  public AmbryStorageQuotaServiceFactory(StorageQuotaConfig config, AccountStatsStore accountStatsStore,
      MetricRegistry metricRegistry) throws Exception {
    storageQuotaService = new AmbryStorageQuotaService(config, accountStatsStore, metricRegistry);
  }

  @Override
  public StorageQuotaService getStorageQuotaService() {
    return storageQuotaService;
  }
}
