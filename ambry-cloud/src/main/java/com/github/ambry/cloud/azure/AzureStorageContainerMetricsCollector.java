/**
 * Copyright 2024 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud.azure;

import com.github.ambry.utils.Utils;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A class that aggregates container metrics. We do _NOT_ want to emit per-container metrics because then the number
 * of metrics increases proportionally overwhelming telemetry. This was attempted and failed.
 *
 * A daemon will run at regular intervals emitting aggregate metrics in a controlled and predictable manner.
 * This is a singleton class to avoid multiple collector threads.
 */
public class AzureStorageContainerMetricsCollector {
  private final AzureMetrics metrics;
  private final ConcurrentHashMap<Long, AzureStorageContainerMetrics> metricMap;
  private final Logger logger;
  private final Runnable collector;
  private final ScheduledExecutorService executor;
  private static AzureStorageContainerMetricsCollector instance;

  private AzureStorageContainerMetricsCollector(AzureMetrics metrics) {
    logger = LoggerFactory.getLogger(AzureStorageContainerMetricsCollector.class);
    metricMap = new ConcurrentHashMap<>();
    this.metrics = metrics;
    collector = () -> {
      Long totalDrift = metricMap.values().stream()
          .map(container -> container.getDrift())
          .reduce(0L, Long::sum);
      this.metrics.azureContainerDriftBytesCount.inc(totalDrift);
    };
    executor = Utils.newScheduler(1, "azure_storage_container_metrics_collector_", true);
    executor.scheduleWithFixedDelay(collector, 0, 2, TimeUnit.MINUTES);
    logger.info("Started AzureStorageContainerMetricsCollector");
  }

  /**
   * Thread-safe singleton initializer
   * @param metrics
   * @return collector instance
   */
  public static synchronized AzureStorageContainerMetricsCollector getInstance(AzureMetrics metrics) {
    if (instance == null) {
      instance = new AzureStorageContainerMetricsCollector(metrics);
    }
    return instance;
  }

  public void addContainer(Long id) {
    metricMap.put(id, new AzureStorageContainerMetrics(id));
  }

  public void removeContainer(Long id) {
    metricMap.remove(id);
  }

  /**
   * Sets the drift of azure-container from ambry-partition.
   * We use a compare-set to guard against accidental multithreaded errors, although two threads will most likely
   * not be responsible for a single partition in VCR. A single thread handles all replicas of a partition.
   * However, we want to avoid any races between reader and writers.
   * Use min() as bootstrapping replicas can give a wrong picture and indicate a large drift even though the partition
   * is fully backed up.
   * @param id
   * @param drift
   */
  public void setContainerDrift(long id, long drift) {
    AzureStorageContainerMetrics azureContainerMetrics = metricMap.get(id);
    Long oldDrift = azureContainerMetrics.getDrift();
    azureContainerMetrics.setDrift(oldDrift, Math.min(oldDrift, drift));
  }
}
