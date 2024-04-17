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
import com.codahale.metrics.Timer;
import com.github.ambry.clustermap.HelixClusterManager.HelixClusterManagerQueryHelper;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Metrics for the {@link HelixClusterManager}
 */
class HelixClusterManagerMetrics {
  private static final Logger logger = LoggerFactory.getLogger(HelixClusterManagerMetrics.class);
  private final HelixClusterManagerQueryHelper clusterMapCallback;
  private final MetricRegistry registry;

  public final Counter liveInstanceChangeTriggerCount;
  public final Counter dataNodeConfigChangeTriggerCount;
  public final Counter idealStateChangeTriggerCount;
  public final Counter instanceConfigChangeTriggerCount;
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
  public final Counter instanceDeleteTriggerCount;

  public Gauge<Long> helixClusterManagerInstantiationFailed;
  public Gauge<Long> helixClusterManagerRemoteInstantiationFailed;
  public Gauge<Long> helixClusterManagerCurrentXid;
  public final Timer routingTableQueryTime;
  public final Counter resourceNameMismatchCount;
  public final Counter paranoidDurabilityIneligibleReplicaCount;

  /**
   * Metrics for the {@link HelixClusterManager}
   * @param registry The {@link MetricRegistry} associated with the {@link HelixClusterManager}
   * @param  clusterMapCallback The {@link HelixClusterManagerQueryHelper} used to query information
   *                            from {@link HelixClusterManager}
   */
  HelixClusterManagerMetrics(MetricRegistry registry, final HelixClusterManagerQueryHelper clusterMapCallback) {
    this.clusterMapCallback = clusterMapCallback;
    this.registry = registry;
    liveInstanceChangeTriggerCount =
        registry.counter(MetricRegistry.name(HelixClusterManager.class, "liveInstanceChangeTriggerCount"));
    dataNodeConfigChangeTriggerCount =
        registry.counter(MetricRegistry.name(HelixClusterManager.class, "dataNodeConfigChangeTriggerCount"));
    idealStateChangeTriggerCount =
        registry.counter(MetricRegistry.name(HelixClusterManager.class, "idealStateChangeTriggerCount"));
    instanceConfigChangeTriggerCount =
        registry.counter(MetricRegistry.name(HelixClusterManager.class, "instanceConfigChangeTriggerCount"));
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
    routingTableQueryTime = registry.timer(MetricRegistry.name(HelixClusterManager.class, "routingTableQueryTime"));
    instanceDeleteTriggerCount =
        registry.counter(MetricRegistry.name(HelixClusterManager.class, "instanceDeleteTriggerCount"));
    resourceNameMismatchCount =
        registry.counter(MetricRegistry.name(HelixClusterManager.class, "resourceNameMismatchCount"));
    paranoidDurabilityIneligibleReplicaCount =
        registry.counter(MetricRegistry.name(HelixClusterManager.class, "ineligibleReplicaCount"));
  }

  void initializeInstantiationMetric(final boolean instantiated, final long instantiationExceptionCount) {
    helixClusterManagerInstantiationFailed = () -> instantiated ? 0L : 1L;
    registry.gauge(MetricRegistry.name(HelixClusterManager.class, "instantiationFailed"),
        () -> helixClusterManagerInstantiationFailed);

    helixClusterManagerRemoteInstantiationFailed = () -> instantiationExceptionCount;
    registry.gauge(MetricRegistry.name(HelixClusterManager.class, "instantiationExceptionCount"),
        () -> helixClusterManagerRemoteInstantiationFailed);
  }

  void initializeXidMetric(final AtomicLong currentXid) {
    helixClusterManagerCurrentXid = currentXid::get;
    registry.gauge(MetricRegistry.name(HelixClusterManager.class, "currentXid"), () -> helixClusterManagerCurrentXid);
  }

  /**
   * Initialize datacenter related metrics.
   */
  void initializeDatacenterMetrics() {
    Gauge<Long> datacenterCount = clusterMapCallback::getDatacenterCount;
    registry.gauge(MetricRegistry.name(HelixClusterManager.class, "datacenterCount"), () -> datacenterCount);
  }

  /**
   * Initialize datanode related metrics.
   */
  void initializeDataNodeMetrics() {
    Gauge<Long> dataNodeCount = clusterMapCallback::getDatanodeCount;
    registry.gauge(MetricRegistry.name(HelixClusterManager.class, "dataNodeCount"), () -> dataNodeCount);

    Gauge<Long> dataNodeDownCount = clusterMapCallback::getDownDatanodesCount;
    registry.gauge(MetricRegistry.name(HelixClusterManager.class, "dataNodeDownCount"), () -> dataNodeDownCount);

    for (final AmbryDataNode datanode : clusterMapCallback.getDatanodes()) {
      final String metricName = datanode.getHostname() + "-" + datanode.getPort() + "-DataNodeResourceState";
      Gauge<Long> dataNodeState = () -> datanode.getState() == HardwareState.AVAILABLE ? 1L : 0L;
      registry.gauge(MetricRegistry.name(HelixClusterManager.class, metricName), () -> dataNodeState);
    }
  }

  /**
   * Initialize disk related metrics.
   */
  void initializeDiskMetrics() {
    Gauge<Long> diskCount = clusterMapCallback::getDiskCount;
    registry.gauge(MetricRegistry.name(HelixClusterManager.class, "diskCount"), () -> diskCount);

    Gauge<Long> diskDownCount = clusterMapCallback::getDownDisksCount;
    registry.gauge(MetricRegistry.name(HelixClusterManager.class, "diskDownCount"), () -> diskDownCount);
  }

  /**
   * Initialize partition related metrics.
   */
  void initializePartitionMetrics() {
    Gauge<Long> partitionCount = clusterMapCallback::getPartitionCount;
    registry.gauge(MetricRegistry.name(HelixClusterManager.class, "partitionCount"), () -> partitionCount);

    Gauge<Long> partitionReadWriteCount = clusterMapCallback::getPartitionReadWriteCount;
    registry.gauge(MetricRegistry.name(HelixClusterManager.class, "partitionReadWriteCount"),
        () -> partitionReadWriteCount);

    Gauge<Long> partitionSealedCount = clusterMapCallback::getPartitionSealedCount;
    registry.gauge(MetricRegistry.name(HelixClusterManager.class, "partitionSealedCount"), () -> partitionSealedCount);

    Gauge<Long> partitionPartiallySealedCount = clusterMapCallback::getPartitionPartiallySealedCount;
    registry.gauge(MetricRegistry.name(HelixClusterManager.class, "partitionPartiallySealedCount"),
        () -> partitionPartiallySealedCount);

    Gauge<Long> isMajorityReplicasDownForAnyPartition = () -> {
      List<ReplicaId> downReplicas = new ArrayList<>();
      for (PartitionId partition : clusterMapCallback.getPartitions()) {
        downReplicas.clear();
        List<? extends ReplicaId> replicas = partition.getReplicaIds();
        int replicaCount = replicas.size();
        int downReplicaCount = 0;
        for (ReplicaId replicaId : replicas) {
          if (replicaId.isDown()) {
            downReplicaCount++;
            downReplicas.add(replicaId);
          }
        }
        if (downReplicaCount > replicaCount / 2) {
          if (logger.isTraceEnabled()) {
            logger.trace("There are more than more of the replicas are down for partition {}, the down replicas are {}",
                partition.toPathString(), downReplicas);
          }
          return 1L;
        }
      }
      return 0L;
    };
    registry.gauge(MetricRegistry.name(HelixClusterManager.class, "isMajorityReplicasDownForAnyPartition"),
        () -> isMajorityReplicasDownForAnyPartition);
  }

  /**
   * Initialize capacity related metrics.
   */
  void initializeCapacityMetrics() {
    Gauge<Long> rawTotalCapacityInBytes = clusterMapCallback::getRawCapacity;
    registry.gauge(MetricRegistry.name(HelixClusterManager.class, "rawTotalCapacityBytes"),
        () -> rawTotalCapacityInBytes);

    Gauge<Long> allocatedRawCapacityInBytes = clusterMapCallback::getAllocatedRawCapacity;
    registry.gauge(MetricRegistry.name(HelixClusterManager.class, "allocatedRawCapacityBytes"),
        () -> allocatedRawCapacityInBytes);

    Gauge<Long> allocatedUsableCapacityInBytes = clusterMapCallback::getAllocatedUsableCapacity;
    registry.gauge(MetricRegistry.name(HelixClusterManager.class, "allocatedUsableCapacityBytes"),
        () -> allocatedUsableCapacityInBytes);
  }

  /**
   * Register FULL AUTO related metrics.
   * @param resources The resources this host belongs to.
   * @param helixClusterManager The {@link HelixClusterManager}.
   */
  void registerMetricsForFullAuto(List<String> resources, HelixClusterManager helixClusterManager) {
    // register resource metrics
    for (String resource : resources) {
      Gauge<Integer> totalInstanceCount = () -> helixClusterManager.getTotalInstanceCount(resource);
      registry.gauge(MetricRegistry.name(HelixClusterManager.class, "Resource_" + resource + "_TotalInstanceCount"),
          () -> totalInstanceCount);

      Gauge<Integer> liveInstanceCount = () -> helixClusterManager.getLiveInstanceCount(resource);
      registry.gauge(MetricRegistry.name(HelixClusterManager.class, "Resource_" + resource + "_LiveInstanceCount"),
          () -> liveInstanceCount);

      Gauge<Long> resourceTotalRegisteredHostDiskCapacity =
          () -> helixClusterManager.getResourceTotalRegisteredHostDiskCapacity(resource);
      registry.gauge(
          MetricRegistry.name(HelixClusterManager.class, "Resource_" + resource + "_TotalRegisteredHostDiskCapacity"),
          () -> resourceTotalRegisteredHostDiskCapacity);

      Gauge<Long> resourceAvailableRegisteredHostDiskCapacity =
          () -> helixClusterManager.getResourceAvailableRegisteredHostDiskCapacity(resource);
      registry.gauge(MetricRegistry.name(HelixClusterManager.class,
              "Resource_" + resource + "_AvailableRegisteredHostDiskCapacity"),
          () -> resourceAvailableRegisteredHostDiskCapacity);

      Gauge<Integer> resourceTotalDiskCapacityUsage =
          () -> helixClusterManager.getResourceTotalDiskCapacityUsage(resource);
      registry.gauge(MetricRegistry.name(HelixClusterManager.class, "Resource_" + resource + "_TotalDiskCapacityUsage"),
          () -> resourceTotalDiskCapacityUsage);

      Gauge<Integer> resourceNumberOfPartitions = () -> helixClusterManager.getNumberOfPartitionsInResource(resource);
      registry.gauge(MetricRegistry.name(HelixClusterManager.class, "Resource_" + resource + "_NumberOfPartitions"),
          () -> resourceNumberOfPartitions);

      // register replica state metrics
      for (ReplicaState replicaState : EnumSet.complementOf(EnumSet.of(ReplicaState.DROPPED))) {
        Gauge<Integer> stateReplicaCountInResource =
            () -> helixClusterManager.getReplicaCountForStateInResource(replicaState, resource);
        registry.gauge(MetricRegistry.name(HelixClusterManager.class,
            "Resource_" + resource + "_ReplicaCountInState" + replicaState.name()), () -> stateReplicaCountInResource);
      }
    }
    // register host metrics
    Gauge<Integer> registeredHostDiskCapacity = helixClusterManager::getRegisteredHostDiskCapacity;
    registry.gauge(MetricRegistry.name(HelixClusterManager.class, "RegisteredHostDiskCapacity"),
        () -> registeredHostDiskCapacity);
    Gauge<Integer> hostReplicaCount = helixClusterManager::getHostReplicaCount;
    registry.gauge(MetricRegistry.name(HelixClusterManager.class, "HostReplicaCount"), () -> hostReplicaCount);
    Gauge<Integer> hostTotalDiskCapacityUsage = helixClusterManager::getHostTotalDiskCapacityUsage;
    registry.gauge(MetricRegistry.name(HelixClusterManager.class, "HostTotalDiskCapacityUsage"),
        () -> hostTotalDiskCapacityUsage);
  }

  /**
   * Deregister FULL AUTO related metrics.
   */
  void deregisterMetricsForFullAuto() {
    Map<String, Gauge> allGauges = registry.getGauges();
    List<String> resourceMetricNames =
        allGauges.keySet().stream().filter(name -> name.contains("Resource_")).collect(Collectors.toList());
    for (String metricName : resourceMetricNames) {
      registry.remove(metricName);
    }

    registry.remove(MetricRegistry.name(HelixClusterManager.class, "RegisteredHostDiskCapacity"));
    registry.remove(MetricRegistry.name(HelixClusterManager.class, "HostReplicaCount"));
    registry.remove(MetricRegistry.name(HelixClusterManager.class, "HostTotalDiskCapacityUsage"));
  }
}
