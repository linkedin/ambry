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
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.StorageQuotaConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.server.AccountStatsStore;
import com.github.ambry.utils.Utils;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * An implementation for {@link StorageQuotaService}.
 */
public class AmbryStorageQuotaService implements StorageQuotaService {
  private static final String STORAGE_QUOTA_SERVICE_PREFIX = "storage-quota-service";

  private final StorageUsageRefresher storageUsageRefresher;
  private final StorageQuotaSource storageQuotaSource;
  private final StorageQuotaEnforcer storageQuotaEnforcer;
  private final ScheduledExecutorService scheduler;
  private final StorageQuotaConfig config;
  private final StorageQuotaServiceMetrics metrics;

  public AmbryStorageQuotaService(VerifiableProperties verifiableProperties, AccountStatsStore accountStatsStore,
      MetricRegistry metricRegistry) throws Exception {
    this.metrics = new StorageQuotaServiceMetrics(metricRegistry);
    this.scheduler = Utils.newScheduler(1, STORAGE_QUOTA_SERVICE_PREFIX, false);
    this.config = new StorageQuotaConfig(verifiableProperties);
    this.storageUsageRefresher = new MySqlStorageUsageRefresher(accountStatsStore, this.scheduler, this.config,
        new ClusterMapConfig(verifiableProperties), metrics);
    this.storageQuotaSource = Utils.getObj(config.sourceFactory, scheduler, config);
    this.storageQuotaEnforcer = new AmbryStorageQuotaEnforcer(null, this.metrics);
  }

  @Override
  public void start() throws Exception {
    storageQuotaEnforcer.setQuotaMode(config.enforcerMode);
    storageQuotaEnforcer.initStorageUsage(storageUsageRefresher.getContainerStorageUsage());
    storageQuotaEnforcer.initStorageQuota(storageQuotaSource.getContainerQuota());
    storageQuotaEnforcer.registerListeners(storageQuotaSource, storageUsageRefresher);
  }

  @Override
  public void shutdown() {
    if (scheduler != null) {
      long schedulerTimeout = Math.max(config.refresherPollingIntervalMs, config.sourcePollingIntervalMs);
      Utils.shutDownExecutorService(scheduler, schedulerTimeout, TimeUnit.MILLISECONDS);
    }
  }

  @Override
  public boolean shouldThrottle(RestRequest restRequest) {
    return this.storageQuotaEnforcer.shouldThrottle(restRequest);
  }

  @Override
  public void setQuotaMode(QuotaMode mode) {
    this.storageQuotaEnforcer.setQuotaMode(mode);
  }
}
