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
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;


public class VcrMetrics {

  private final MetricRegistry registry;

  // Encryption metrics
  public final Counter blobEncryptionCount;
  public final Counter blobDecryptionCount;
  public final Timer blobEncryptionTime;
  public final Timer blobDecryptionTime;

  public VcrMetrics(MetricRegistry registry) {
    this.registry = registry;
    blobEncryptionCount = registry.counter(MetricRegistry.name(CloudBlobStore.class, "BlobEncryptionCount"));
    blobDecryptionCount = registry.counter(MetricRegistry.name(CloudBlobStore.class, "BlobDecryptionCount"));
    blobEncryptionTime = registry.timer(MetricRegistry.name(CloudBlobStore.class, "BlobEncryptionTime"));
    blobDecryptionTime = registry.timer(MetricRegistry.name(CloudBlobStore.class, "BlobDecryptionTime"));
  }

  /**
   * @return the {@link MetricRegistry} where these metrics are registered.
   */
  public MetricRegistry getMetricRegistry() {
    return registry;
  }
}
