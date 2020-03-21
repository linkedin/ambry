/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.clustermap;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Metrics for the {@link HelixClusterManager}
 */
class HelixClusterManagerMetrics {
  private final HelixClusterManager.HelixClusterManagerCallback clusterMapCallback;
  private final MetricRegistry registry;

  public final Counter liveInstanceChangeTriggerCount;
  public final Counter instanceConfigChangeTriggerCount;
  public final Counter idealStateChangeTriggerCount;
  public final Counter routingTableChangeTriggerCount;
  public final Counter getPartitionIdFromStreamMismatchCount;
  public final Counter getWritablePartitionIdsMismatchCount;
  public final Counter getAllPartitionIdsMismatchCount;
  public final Counter hasDatacenterMismatchCount;
  public final Counter getDatacenterNameMismatchCount;
  public final Counter getDataNodeIdMismatchCount;
  public final Counter getReplicaIdsMismatchCount;
  public final Counter getDataNodeIdsMismatchCount;
  public final Counter ignoredUpdatesCount;
  public final Counter instanceConfigChangeErrorCount;

  public Gauge<Long> helixClusterManagerInstantiationFailed;
  public Gauge<Long> helixClusterManagerRemoteInstantiationFailed;
  public Gauge<Long> helixClusterManagerCurrentXid;

  /**
   * Metrics for the {@link HelixClusterManager}
   * @param registry The {@link MetricRegistry} associated with the {@link HelixClusterManager}
   * @param  clusterMapCallback The {@link HelixClusterManager.HelixClusterManagerCallback} used to query information
   *                            from {@link HelixClusterManager}
   */
  HelixClusterManagerMetrics(MetricRegistry registry,
      final HelixClusterManager.HelixClusterManagerCallback clusterMapCallback) {
    this.clusterMapCallback = clusterMapCallback;
    this.registry = registry;
    liveInstanceChangeTriggerCount =
        registry.counter(MetricRegistry.name(HelixClusterManager.class, "liveInstanceChangeTriggerCount"));
    instanceConfigChangeTriggerCount =
        registry.counter(MetricRegistry.name(HelixClusterManager.class, "instanceConfigChangeTriggerCount"));
    idealStateChangeTriggerCount =
        registry.counter(MetricRegistry.name(HelixClusterManager.class, "idealStateChangeTriggerCount"));
    routingTableChangeTriggerCount =
        registry.counter(MetricRegistry.name(HelixClusterManager.class, "routingTableChangeTriggerCount"));
    getPartitionIdFromStreamMismatchCount =
        registry.counter(MetricRegistry.name(HelixClusterManager.class, "getPartitionIdFromStreamMismatchCount"));
    getWritablePartitionIdsMismatchCount =
        registry.counter(MetricRegistry.name(HelixClusterManager.class, "getWritablePartitionIdsMismatchCount"));
    getAllPartitionIdsMismatchCount =
        registry.counter(MetricRegistry.name(HelixClusterManager.class, "getAllPartitionIdsMismatchCount"));
    hasDatacenterMismatchCount =
        registry.counter(MetricRegistry.name(HelixClusterManager.class, "hasDatacenterMismatchCount"));
    getDatacenterNameMismatchCount =
        registry.counter(MetricRegistry.name(HelixClusterManager.class, "getDatacenterNameMismatchCount"));
    getDataNodeIdMismatchCount =
        registry.counter(MetricRegistry.name(HelixClusterManager.class, "getDataNodeIdMismatchCount"));
    getReplicaIdsMismatchCount =
        registry.counter(MetricRegistry.name(HelixClusterManager.class, "getReplicaIdsMismatchCount"));
    getDataNodeIdsMismatchCount =
        registry.counter(MetricRegistry.name(HelixClusterManager.class, "getDataNodeIdsMismatchCount"));
    ignoredUpdatesCount = registry.counter(MetricRegistry.name(HelixClusterManager.class, "ignoredUpdatesCount"));
    instanceConfigChangeErrorCount =
        registry.counter(MetricRegistry.name(HelixClusterManager.class, "instanceConfigChangeErrorCount"));
  }

  void initializeInstantiationMetric(final boolean instantiated, final long instantiationExceptionCount) {
    helixClusterManagerInstantiationFailed = () -> instantiated ? 0L : 1L;
    registry.register(MetricRegistry.name(HelixClusterManager.class, "instantiationFailed"),
        helixClusterManagerInstantiationFailed);

    helixClusterManagerRemoteInstantiationFailed = () -> instantiationExceptionCount;
    registry.register(MetricRegistry.name(HelixClusterManager.class, "instantiationExceptionCount"),
        helixClusterManagerRemoteInstantiationFailed);
  }

  void initializeXidMetric(final AtomicLong currentXid) {
    helixClusterManagerCurrentXid = currentXid::get;
    registry.register(MetricRegistry.name(HelixClusterManager.class, "currentXid"), helixClusterManagerCurrentXid);
  }

  /**
   * Initialize datacenter related metrics.
   */
  void initializeDatacenterMetrics() {
    Gauge<Long> datacenterCount = clusterMapCallback::getDatacenterCount;
    registry.register(MetricRegistry.name(HelixClusterManager.class, "datacenterCount"), datacenterCount);
  }

  /**
   * Initialize datanode related metrics.
   */
  void initializeDataNodeMetrics() {
    Gauge<Long> dataNodeCount = clusterMapCallback::getDatanodeCount;
    registry.register(MetricRegistry.name(HelixClusterManager.class, "dataNodeCount"), dataNodeCount);

    Gauge<Long> dataNodeDownCount = clusterMapCallback::getDownDatanodesCount;
    registry.register(MetricRegistry.name(HelixClusterManager.class, "dataNodeDownCount"), dataNodeDownCount);

    for (final AmbryDataNode datanode : clusterMapCallback.getDatanodes()) {
      final String metricName = datanode.getHostname() + "-" + datanode.getPort() + "-DataNodeResourceState";
      Gauge<Long> dataNodeState = () -> datanode.getState() == HardwareState.AVAILABLE ? 1L : 0L;
      registry.register(MetricRegistry.name(HelixClusterManager.class, metricName), dataNodeState);
    }
  }

  /**
   * Initialize disk related metrics.
   */
  void initializeDiskMetrics() {
    Gauge<Long> diskCount = clusterMapCallback::getDiskCount;
    registry.register(MetricRegistry.name(HelixClusterManager.class, "diskCount"), diskCount);

    Gauge<Long> diskDownCount = clusterMapCallback::getDownDisksCount;
    registry.register(MetricRegistry.name(HelixClusterManager.class, "diskDownCount"), diskDownCount);

    for (final AmbryDisk disk : clusterMapCallback.getDisks(null)) {
      final String metricName =
          disk.getDataNode().getHostname() + "-" + disk.getDataNode().getPort() + "-" + disk.getMountPath()
              + "-DiskResourceState";
      Gauge<Long> diskState = () -> disk.getState() == HardwareState.AVAILABLE ? 1L : 0L;
      registry.register(MetricRegistry.name(HelixClusterManager.class, metricName), diskState);
    }
  }

  /**
   * Initialize partition related metrics.
   */
  void initializePartitionMetrics() {
    Gauge<Long> partitionCount = clusterMapCallback::getPartitionCount;
    registry.register(MetricRegistry.name(HelixClusterManager.class, "partitionCount"), partitionCount);

    Gauge<Long> partitionReadWriteCount = clusterMapCallback::getPartitionReadWriteCount;
    registry.register(MetricRegistry.name(HelixClusterManager.class, "partitionReadWriteCount"),
        partitionReadWriteCount);

    Gauge<Long> partitionSealedCount = clusterMapCallback::getPartitionSealedCount;
    registry.register(MetricRegistry.name(HelixClusterManager.class, "partitionSealedCount"), partitionSealedCount);

    Gauge<Long> isMajorityReplicasDownForAnyPartition = () -> {
      for (PartitionId partition : clusterMapCallback.getPartitions()) {
        List<? extends ReplicaId> replicas = partition.getReplicaIds();
        int replicaCount = replicas.size();
        int downReplicas = 0;
        for (ReplicaId replicaId : replicas) {
          if (replicaId.isDown()) {
            downReplicas++;
          }
        }
        if (downReplicas > replicaCount / 2) {
          return 1L;
        }
      }
      return 0L;
    };
    registry.register(MetricRegistry.name(HelixClusterManager.class, "isMajorityReplicasDownForAnyPartition"),
        isMajorityReplicasDownForAnyPartition);
  }

  /**
   * Initialize capacity related metrics.
   */
  void initializeCapacityMetrics() {
    Gauge<Long> rawTotalCapacityInBytes = clusterMapCallback::getRawCapacity;
    registry.register(MetricRegistry.name(HelixClusterManager.class, "rawTotalCapacityBytes"), rawTotalCapacityInBytes);
    Gauge<Long> allocatedRawCapacityInBytes = clusterMapCallback::getAllocatedRawCapacity;
    registry.register(MetricRegistry.name(HelixClusterManager.class, "allocatedRawCapacityBytes"),
        allocatedRawCapacityInBytes);

    Gauge<Long> allocatedUsableCapacityInBytes = clusterMapCallback::getAllocatedUsableCapacity;
    registry.register(MetricRegistry.name(HelixClusterManager.class, "allocatedUsableCapacityBytes"),
        allocatedUsableCapacityInBytes);
  }
}
