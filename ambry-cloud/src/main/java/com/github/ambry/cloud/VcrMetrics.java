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
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;


public class VcrMetrics {

  // Encryption metrics
  public final Counter blobEncryptionCount;
  public final Counter blobDecryptionCount;
  public final Counter blobEncryptionErrorCount;
  public final Counter blobDecryptionErrorCount;
  public final Timer blobEncryptionTime;
  public final Timer blobDecryptionTime;
  // Compaction metrics
  // Rate of blob compaction for VCR instance
  public final Meter blobCompactionRate;
  public final Meter deprecatedContainerBlobCompactionRate;
  public final Counter compactionFailureCount;
  public final Counter compactionShutdownTimeoutCount;
  // Cache counters
  public final Counter blobCacheLookupCount;
  public final Counter blobCacheHitCount;
  // Error counters
  public final Counter blobUploadSkippedCount;
  public final Counter updateTtlNotSetError;
  public final Counter addPartitionErrorCount;
  public final Counter removePartitionErrorCount;
  public final Counter tokenReloadWarnCount;
  // Container deprecation metrics
  public final Timer accountServiceFetchTime;
  public final Timer deprecationTaskRunTime;
  public final Counter deprecatedContainerCount;
  public final Counter deprecationSyncTaskRegistrationFailureCount;

  // Retry metrics
  /** Number of msec spent waiting between retries */
  public final Counter retryWaitTimeMsec;
  /** Number of times operation was retried */
  public final Counter retryCount;

  // VCR Automation
  public final Counter vcrHelixUpdateFailCount;
  public final Counter vcrHelixUpdateSuccessCount;
  public Gauge<Integer> vcrHelixUpdaterGauge;
  public Gauge<Integer> vcrHelixUpdateInProgressGauge;
  public final Counter vcrHelixNotOnSync;
  private final MetricRegistry registry;

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
    blobCacheLookupCount = registry.counter(MetricRegistry.name(CloudBlobStore.class, "BlobCacheLookupCount"));
    blobCacheHitCount = registry.counter(MetricRegistry.name(CloudBlobStore.class, "BlobCacheHitCount"));
    retryCount = registry.counter(MetricRegistry.name(CloudBlobStore.class, "RetryCount"));
    retryWaitTimeMsec = registry.counter(MetricRegistry.name(CloudBlobStore.class, "RetryWaitTimeMsec"));
    blobCompactionRate = registry.meter(MetricRegistry.name(CloudStorageCompactor.class, "BlobCompactionRate"));
    deprecatedContainerBlobCompactionRate =
        registry.meter(MetricRegistry.name(CloudContainerCompactor.class, "DeprecatedContainerBlobCompactionRate"));
    compactionFailureCount =
        registry.counter(MetricRegistry.name(CloudStorageCompactor.class, "CompactionFailureCount"));
    compactionShutdownTimeoutCount =
        registry.counter(MetricRegistry.name(CloudStorageCompactor.class, "CompactionShutdownTimeoutCount"));
    addPartitionErrorCount =
        registry.counter(MetricRegistry.name(VcrReplicationManager.class, "AddPartitionErrorCount"));
    removePartitionErrorCount =
        registry.counter(MetricRegistry.name(VcrReplicationManager.class, "RemovePartitionErrorCount"));
    tokenReloadWarnCount = registry.counter(MetricRegistry.name(VcrReplicationManager.class, "TokenReloadWarnCount"));
    deprecationTaskRunTime =
        registry.timer(MetricRegistry.name(DeprecatedContainerCloudSyncTask.class, "DeprecationTaskRunTime"));
    accountServiceFetchTime =
        registry.timer(MetricRegistry.name(DeprecatedContainerCloudSyncTask.class, "AccountServiceFetchTime"));
    deprecatedContainerCount =
        registry.counter(MetricRegistry.name(DeprecatedContainerCloudSyncTask.class, "DeprecatedContainerCount"));
    deprecationSyncTaskRegistrationFailureCount = registry.counter(
        MetricRegistry.name(DeprecatedContainerCloudSyncTask.class, "DeprecationSyncTaskRegistrationFailureCount"));
    vcrHelixUpdateFailCount =
        registry.counter(MetricRegistry.name(VcrReplicationManager.class, "VcrHelixUpdateFailCount"));
    vcrHelixUpdateSuccessCount =
        registry.counter(MetricRegistry.name(VcrReplicationManager.class, "VcrHelixUpdateSuccessCount"));
    vcrHelixNotOnSync =
        registry.counter(MetricRegistry.name(VcrReplicationManager.class, "VcrHelixNotOnSync"));
  }

  /**
   * @return the {@link MetricRegistry} where these metrics are registered.
   */
  public MetricRegistry getMetricRegistry() {
    return registry;
  }

  public void registerVcrHelixUpdateGauge(Gauge<Integer> updaterGaugeCount,
      Gauge<Integer> vcrHelixUpdateInProgressGaugeCount) {
    vcrHelixUpdaterGauge = registry.gauge(MetricRegistry.name(VcrReplicationManager.class, "VcrHelixUpdaterGauge"),
        () -> updaterGaugeCount);
    vcrHelixUpdateInProgressGauge =
        registry.gauge(MetricRegistry.name(VcrReplicationManager.class, "VcrHelixUpdateInProgressGauge"),
            () -> vcrHelixUpdateInProgressGaugeCount);
  }
}
