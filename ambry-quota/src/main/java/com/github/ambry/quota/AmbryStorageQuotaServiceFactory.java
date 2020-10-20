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
package com.github.ambry.quota;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.commons.CommonUtils;
import com.github.ambry.config.StorageQuotaConfig;
import com.github.ambry.utils.Utils;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;


/**
 * An factory implementation for {@link StorageQuotaService}. It uses {@link HelixStorageUsageRefresher}, {@link JSONStringStorageQuotaSource}
 * and {@link AmbryStorageQuotaEnforcer}.
 */
public class AmbryStorageQuotaServiceFactory implements StorageQuotaServiceFactory {
  private final StorageQuotaService storageQuotaService;

  /**
   * Constructor to create {@link AmbryStorageQuotaServiceFactory}.
   * @param storageQuotaConfig The {@link StorageQuotaConfig}.
   * @param metricRegistry The {@link MetricRegistry} to register new metrics.
   * @throws Exception
   */
  public AmbryStorageQuotaServiceFactory(StorageQuotaConfig storageQuotaConfig, MetricRegistry metricRegistry)
      throws Exception {
    storageQuotaService = new AmbryStorageQuotaService(storageQuotaConfig);
  }

  @Override
  public StorageQuotaService getStorageQuotaService() {
    return storageQuotaService;
  }
}
