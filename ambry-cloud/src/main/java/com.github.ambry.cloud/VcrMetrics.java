/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;


public class VcrMetrics {

  private final MetricRegistry registry;

  // Encryption metrics
  public final Counter blobEncryptionCount;
  public final Counter blobDecryptionCount;
  public final Counter blobEncryptionErrorCount;
  public final Counter blobDecryptionErrorCount;
  public final Timer blobEncryptionTime;
  public final Timer blobDecryptionTime;
  // Time to run compaction task
  public final Timer blobCompactionTime;
  // Error counters
  public final Counter blobUploadSkippedCount;
  public final Counter updateTtlNotSetError;
  public final Counter addPartitionErrorCount;
  public final Counter removePartitionErrorCount;
  public final Counter tokenReloadWarnCount;

  public VcrMetrics(MetricRegistry registry) {
    this.registry = registry;
    blobEncryptionCount = registry.counter(MetricRegistry.name(CloudBlobStore.class, "BlobEncryptionCount"));
    blobDecryptionCount = registry.counter(MetricRegistry.name(CloudBlobStore.class, "BlobDecryptionCount"));
    blobEncryptionErrorCount = registry.counter(MetricRegistry.name(CloudBlobStore.class, "BlobEncryptionErrorCount"));
    blobDecryptionErrorCount = registry.counter(MetricRegistry.name(CloudBlobStore.class, "BlobDecryptionErrorCount"));
    blobEncryptionTime = registry.timer(MetricRegistry.name(CloudBlobStore.class, "BlobEncryptionTime"));
    blobDecryptionTime = registry.timer(MetricRegistry.name(CloudBlobStore.class, "BlobDecryptionTime"));
    blobUploadSkippedCount = registry.counter(MetricRegistry.name(CloudBlobStore.class, "BlobUploadSkippedCount"));
    updateTtlNotSetError = registry.counter(MetricRegistry.name(CloudBlobStore.class, "UpdateTtlNotSetError"));
    blobCompactionTime = registry.timer(MetricRegistry.name(CloudBlobStore.class, "BlobCompactionTime"));
    addPartitionErrorCount =
        registry.counter(MetricRegistry.name(VcrReplicationManager.class, "AddPartitionErrorCount"));
    removePartitionErrorCount =
        registry.counter(MetricRegistry.name(VcrReplicationManager.class, "RemovePartitionErrorCount"));
    tokenReloadWarnCount = registry.counter(MetricRegistry.name(VcrReplicationManager.class, "TokenReloadWarnCount"));
  }

  /**
   * @return the {@link MetricRegistry} where these metrics are registered.
   */
  public MetricRegistry getMetricRegistry() {
    return registry;
  }
}
