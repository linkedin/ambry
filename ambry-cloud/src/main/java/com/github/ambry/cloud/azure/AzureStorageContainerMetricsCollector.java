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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.replication.RemoteReplicaInfo;
import com.github.ambry.utils.Utils;
import java.util.List;
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
  private final ScheduledExecutorService executor;
  private static AzureStorageContainerMetricsCollector instance;
  private final VerifiableProperties properties;

  public static final Logger logger = LoggerFactory.getLogger(AzureStorageContainerMetricsCollector.class);

  private AzureStorageContainerMetricsCollector(MetricRegistry metrics, VerifiableProperties properties) {
    metricMap = new ConcurrentHashMap<>();
    this.metrics = new AzureMetrics(metrics);
    this.properties = properties;
    executor = Utils.newScheduler(1, "azure_storage_container_metrics_collector_", true);
    executor.scheduleWithFixedDelay(getCollector(), 0, 2, TimeUnit.MINUTES);
    logger.info("Started AzureStorageContainerMetricsCollector");
  }

  private Runnable getCollector() {
    return () -> {
      Long totalLag = metricMap.values().stream()
          .map(container -> container.getPartitionLag())
          .reduce(0L, Long::sum);
      this.metrics.azureContainerLagBytesCount.inc(totalLag);
    };
  }

  /**
   * Thread-safe singleton initializer
   * @param metrics
   * @return collector instance
   */
  public static synchronized AzureStorageContainerMetricsCollector getInstance(MetricRegistry metrics,
      VerifiableProperties properties) {
    if (instance == null) {
      instance = new AzureStorageContainerMetricsCollector(metrics, properties);
    }
    return instance;
  }

  public void removePartitionReplicas(List<RemoteReplicaInfo> remoteReplicaInfos) {
    for (RemoteReplicaInfo rinfo : remoteReplicaInfos) {
      long pid = rinfo.getReplicaId().getPartitionId().getId();
      String rid = rinfo.getReplicaId().getDataNodeId().getHostname();
      if (metricMap.containsKey(pid)) {
        metricMap.get(pid).removePartitionReplica(rid);
      }
    }
  }

  public void removePartition(Long id) {
    metricMap.remove(id);
  }

  /**
   * Sets the lag of azure-container from ambry-partition.
   * We use a compare-set to guard against accidental multithreaded errors, although two threads will most likely
   * not be responsible for a single partition in VCR. A single thread handles all replicas of a partition.
   * However, we want to avoid any races between reader and writers.
   * Use min() as bootstrapping replicas can give a wrong picture and indicate a large lag even though the partition
   * is fully backed up in Azure.
   * @param rinfo RemoteReplicaInfo
   * @param lag Lag in bytes
   */
  public synchronized void setPartitionReplicaLag(RemoteReplicaInfo rinfo, long lag) {
    long pid = rinfo.getReplicaId().getPartitionId().getId();
    String rid = rinfo.getReplicaId().getDataNodeId().getHostname();
    metricMap.compute(pid, (k, v) -> v == null ? new AzureStorageContainerMetrics(pid).setPartitionReplicaLag(rid, lag)
        : v.setPartitionReplicaLag(rid, lag));
  }
}
