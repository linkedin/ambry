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
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import java.util.concurrent.TimeUnit;


/**
 * Metrics for Disk operations
 */
public class DiskMetrics {
  private static final String SEPARATOR = ".";
  public final MetricRegistry registry;
  public final Histogram diskReadTimePerMbInMs;
  public final Histogram diskWriteTimePerMbInMs;
  public final Meter diskCompactionCopyRateInBytes;
  public final Counter diskCompactionErrorDueToDiskFailureCount;

  public DiskMetrics(MetricRegistry registry, String diskMountPath, int diskIoHistogramReservoirTimeWindow) {
    this.registry = registry;
    // Should be initialized only once per disk.
    String prefix = diskMountPath + SEPARATOR;
    diskReadTimePerMbInMs = registry.histogram(MetricRegistry.name(BlobStore.class, prefix + "DiskReadTimePerMbInMs"),
        () -> new Histogram(
            new SlidingTimeWindowArrayReservoir(diskIoHistogramReservoirTimeWindow, TimeUnit.MILLISECONDS)));

    diskWriteTimePerMbInMs = registry.histogram(MetricRegistry.name(BlobStore.class, prefix + "DiskWriteTimePerMbInMs"),
        () -> new Histogram(
            new SlidingTimeWindowArrayReservoir(diskIoHistogramReservoirTimeWindow, TimeUnit.MILLISECONDS)));
    diskCompactionCopyRateInBytes =
        registry.meter(MetricRegistry.name(BlobStoreCompactor.class, prefix + "DiskCompactionCopyRateInBytes"));
    diskCompactionErrorDueToDiskFailureCount =
        registry.counter(MetricRegistry.name(BlobStoreCompactor.class, prefix + "DiskCompactionErrorDueToDiskFailureCount"));
  }
}
