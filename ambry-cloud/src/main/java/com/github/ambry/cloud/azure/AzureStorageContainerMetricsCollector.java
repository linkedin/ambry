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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class AzureStorageContainerMetricsCollector implements Runnable {
  private final AzureMetrics azureMetrics;
  private final ConcurrentHashMap<Long, AzureStorageContainerMetrics> azureContainerMetricsMap;
  ScheduledExecutorService executor;

  public AzureStorageContainerMetricsCollector(AzureMetrics metrics) {
    azureContainerMetricsMap = new ConcurrentHashMap<>();
    azureMetrics = metrics;
    executor = Utils.newScheduler(1, "azure_storage_container_metrics_collector_", true);
    executor.scheduleWithFixedDelay(this::run, 0, 2, TimeUnit.MINUTES);
  }

  @Override
  public void run() {
    Long totalDrift = 0L;
    for (Map.Entry<Long, AzureStorageContainerMetrics> entry : azureContainerMetricsMap.entrySet()) {
      AzureStorageContainerMetrics azureContainerMetrics = entry.getValue();
      totalDrift += azureContainerMetrics.getDrift();
    }
    azureMetrics.azureContainerDriftBytesCount.inc(totalDrift);
  }

  public void addContainer(Long id) {
    azureContainerMetricsMap.put(id, new AzureStorageContainerMetrics(id));
  }

  public void removeContainer(Long id) {
    azureContainerMetricsMap.remove(id);
  }

  public void setContainerDrift(long id, long drift) {
    AzureStorageContainerMetrics azureContainerMetrics = azureContainerMetricsMap.get(id);
    Long oldDrift = azureContainerMetrics.getDrift();
    azureContainerMetrics.compareAndSet(oldDrift, Math.min(oldDrift, drift));
  }
}
