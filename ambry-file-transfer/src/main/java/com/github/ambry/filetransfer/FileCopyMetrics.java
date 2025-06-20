/**
 * Copyright 2025 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.filetransfer;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.github.ambry.config.FileCopyBasedReplicationConfig;
import com.github.ambry.filetransfer.handler.StoreFileCopyHandler;
import java.util.concurrent.TimeUnit;


/**
 * File copy related metrics
 */
public class FileCopyMetrics {
  private final Counter partitionsInFileCopyPath;
  private final Counter partitionsFileCopyInitiated;
  private final Counter partitionsFileCopySkipped;
  private final Counter partitionFileCopySuccess;
  private final Counter partitionFileCopyFailure;
  private final Counter fileCopyRunningThreadCount;
  private final Histogram fileCopyPerPartitionTimeMs;
  private final Histogram fileCopyEligibleDataPerPartitionInBytes;
  private final Histogram fileCopyAverageSpeedPerPartition;

  public FileCopyMetrics(MetricRegistry registry, int fileCopyMetricsReservoirTimeWindowMs) {
    partitionsInFileCopyPath =
        registry.counter(MetricRegistry.name(FileCopyBasedReplicationManager.class, "PartitionsInFileCopyPath"));
    partitionsFileCopyInitiated =
        registry.counter(MetricRegistry.name(FileCopyBasedReplicationManager.class, "PartitionsFileCopyInitiated"));
    partitionsFileCopySkipped =
        registry.counter(MetricRegistry.name(FileCopyBasedReplicationManager.class, "PartitionsFileCopySkipped"));
    partitionFileCopySuccess =
        registry.counter(MetricRegistry.name(FileCopyBasedReplicationManager.class, "PartitionsFileCopySuccess"));
    partitionFileCopyFailure =
        registry.counter(MetricRegistry.name(FileCopyBasedReplicationManager.class, "PartitionsFileCopyFailure"));
    fileCopyRunningThreadCount =
        registry.counter(MetricRegistry.name(FileCopyThread.class, "FileCopyRunningThreadCount"));

    fileCopyPerPartitionTimeMs =
        registry.histogram(MetricRegistry.name(FileCopyThread.class, "FileCopyPerPartitionTimeMs"), () -> new Histogram(
            new SlidingTimeWindowArrayReservoir(fileCopyMetricsReservoirTimeWindowMs, TimeUnit.MILLISECONDS)));

    fileCopyEligibleDataPerPartitionInBytes =
        registry.histogram(MetricRegistry.name(StoreFileCopyHandler.class, "FileCopyEligibleDataPerPartitionInBytes"),
            () -> new Histogram(
                new SlidingTimeWindowArrayReservoir(fileCopyMetricsReservoirTimeWindowMs, TimeUnit.MILLISECONDS)));

    fileCopyAverageSpeedPerPartition =
        registry.histogram(MetricRegistry.name(StoreFileCopyHandler.class, "FileCopyAverageSpeedPerPartition"),
            () -> new Histogram(
                new SlidingTimeWindowArrayReservoir(fileCopyMetricsReservoirTimeWindowMs, TimeUnit.MILLISECONDS)));
  }

  public FileCopyMetrics(MetricRegistry registry) {
    this(registry, Integer.parseInt(FileCopyBasedReplicationConfig.DefaultFileCopyMetricReservoirTimeWindowMs));
  }

  public void incrementFileCopyInitiated() {
    partitionsFileCopyInitiated.inc();
  }

  public void incrementFileCopySkipped() {
    partitionsFileCopySkipped.inc();
  }

  public void incrementPartitionInFileCopyPath() {
    partitionsInFileCopyPath.inc();
  }

  public void decrementPartitionInFileCopyPath() {
    partitionsInFileCopyPath.dec();
  }

  public void incrementPartitionFileCopySuccess() {
    partitionFileCopySuccess.inc();
  }

  public void incrementPartitionFileCopyFailure() {
    partitionFileCopyFailure.inc();
  }

  public void incrementFileCopyRunningThreadCount() {
    fileCopyRunningThreadCount.inc();
  }

  public void decrementFileCopyRunningThreadCount() {
    fileCopyRunningThreadCount.dec();
  }

  public void updateFileCopyPerPartitionTimeMs(long timeTaken) {
    fileCopyPerPartitionTimeMs.update(timeTaken);
  }

  public void updateFileCopyDataPerPartitionInBytes(long bytes) {
    fileCopyEligibleDataPerPartitionInBytes.update(bytes);
  }

  public void updateFileCopyAverageSpeedPerPartition(long bytesPerSec) {
    fileCopyAverageSpeedPerPartition.update(bytesPerSec);
  }
}
