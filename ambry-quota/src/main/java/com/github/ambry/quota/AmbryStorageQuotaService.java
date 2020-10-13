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

import com.github.ambry.config.StorageQuotaConfig;
import com.github.ambry.utils.Utils;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


/**
 * An implementation for {@link StorageQuotaService}.
 */
public class AmbryStorageQuotaService implements StorageQuotaService {

  private final StorageUsageRefresher storageUsageRefresher;
  private final StorageQuotaSource storageQuotaSource;
  private final StorageQuotaEnforcer storageQuotaEnforcer;
  private final ScheduledExecutorService scheduler;
  private final StorageQuotaConfig config;

  public AmbryStorageQuotaService(StorageUsageRefresher storageUsageRefresher, StorageQuotaSource storageQuotaSource,
      StorageQuotaEnforcer storageQuotaEnforcer, ScheduledExecutorService scheduler, StorageQuotaConfig config) {
    this.storageUsageRefresher = Objects.requireNonNull(storageUsageRefresher, "StorageUsageRefresher empty");
    this.storageQuotaSource = Objects.requireNonNull(storageQuotaSource, "StorageQuotaSource empty");
    this.storageQuotaEnforcer = Objects.requireNonNull(storageQuotaEnforcer, "StorageQuotaEnforcer empty");
    this.config = Objects.requireNonNull(config, "StorageQuotaConfig empty");
    this.scheduler = scheduler;
  }

  @Override
  public void start() throws Exception {
    storageQuotaEnforcer.initStorageUsage(storageUsageRefresher.getContainerStorageUsage());
    storageQuotaEnforcer.initStorageQuota(storageQuotaSource.getContainerQuota());

    StorageQuotaSource.Listener sourceListener = storageQuotaEnforcer.getQuotaSourceListener();
    if (sourceListener != null) {
      storageQuotaSource.registerListener(sourceListener);
    }
    StorageUsageRefresher.Listener refresherListener = storageQuotaEnforcer.getUsageRefresherListener();
    if (refresherListener != null) {
      storageUsageRefresher.registerListener(refresherListener);
    }
  }

  @Override
  public void shutdown() {
    if (scheduler != null) {
      long schedulerTimeout = Math.max(config.refresherPollingIntervalMs, config.sourcePollingIntervalMs);
      Utils.shutDownExecutorService(scheduler, schedulerTimeout, TimeUnit.MILLISECONDS);
    }
  }

  @Override
  public boolean shouldThrottle(short accountId, short containerId, QuotaOperation op, long size) {
    return this.storageQuotaEnforcer.shouldThrottle(accountId, containerId, op, size);
  }

  @Override
  public void setQuotaMode(QuotaMode mode) {
    this.storageQuotaEnforcer.setQuotaMode(mode);
  }
}
