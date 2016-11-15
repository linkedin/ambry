/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.store;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;


/**
 * Metrics for all of the stores on a node.
 */
public class StorageManagerMetrics {
  private final MetricRegistry registry;

  public final Timer diskStartTime;
  public final Counter totalStoreStartFailures;
  public final Counter diskMountPathFailures;

  /**
   * Create a {@link StorageManagerMetrics} object for handling metrics related to the stores on a node.
   * @param registry the {@link MetricRegistry} to use.
   */
  public StorageManagerMetrics(MetricRegistry registry) {
    this.registry = registry;
    diskStartTime = registry.timer(MetricRegistry.name(StorageManager.class, "DiskStartTime"));
    totalStoreStartFailures = registry.counter(MetricRegistry.name(StorageManager.class, "TotalStoreStartFailures"));
    diskMountPathFailures = registry.counter(MetricRegistry.name(StorageManager.class, "DiskMountPathFailures"));
  }

  /**
   * Create a {@link StoreMetrics} object for handling metrics related to a specific store.
   * @param storeId the name of the store.
   * @return the {@link StoreMetrics}
   */
  public StoreMetrics createStoreMetrics(String storeId) {
    return new StoreMetrics(storeId, registry);
  }
}
