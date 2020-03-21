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
package com.github.ambry.clustermap;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import java.util.ArrayList;
import java.util.List;


/**
 * Metrics for the {@link StaticClusterManager}.
 */
class ClusterMapMetrics {
  private MetricRegistry registry;
  private final HardwareLayout hardwareLayout;
  private final PartitionLayout partitionLayout;

  public final Gauge<Long> hardwareLayoutVersion;
  public final Gauge<Long> partitionLayoutVersion;

  public final Gauge<Long> datacenterCount;
  public final Gauge<Long> dataNodeCount;
  public final Gauge<Long> diskCount;

  public final Gauge<Long> dataNodesHardUpCount;
  public final Gauge<Long> dataNodesHardDownCount;
  public final Gauge<Long> dataNodesUnavailableCount;
  public List<Gauge<Long>> dataNodeStateList;

  public final Gauge<Long> disksHardUpCount;
  public final Gauge<Long> disksHardDownCount;
  public final Gauge<Long> disksUnavailableCount;
  public List<Gauge<Long>> diskStateList;

  public final Gauge<Long> partitionCount;
  public final Gauge<Long> partitionsReadWrite;
  public final Gauge<Long> partitionsReadOnly;

  public final Gauge<Boolean> isMajorityReplicasDown;

  public final Gauge<Long> rawCapacityInBytes;
  public final Gauge<Long> allocatedRawCapacityInBytes;
  public final Gauge<Long> allocatedUsableCapacityInBytes;

  /**
   * Metrics for the {@link StaticClusterManager}
   * @param hardwareLayout The {@link HardwareLayout} associated with the {@link StaticClusterManager}
   * @param partitionLayout The {@link PartitionLayout} associated with the {@link StaticClusterManager}
   * @param registry The {@link MetricRegistry} associated with the {@link StaticClusterManager}
   */
  public ClusterMapMetrics(HardwareLayout hardwareLayout, PartitionLayout partitionLayout, MetricRegistry registry) {
    this.registry = registry;
    this.hardwareLayout = hardwareLayout;
    this.partitionLayout = partitionLayout;

    // Metrics based on HardwareLayout

    this.hardwareLayoutVersion = this::getHardwareLayoutVersion;
    this.partitionLayoutVersion = this::getPartitionLayoutVersion;
    registry.register(MetricRegistry.name(ClusterMap.class, "hardwareLayoutVersion"), hardwareLayoutVersion);
    registry.register(MetricRegistry.name(ClusterMap.class, "partitionLayoutVersion"), partitionLayoutVersion);

    this.datacenterCount = this::countDatacenters;
    this.dataNodeCount = this::countDataNodes;
    this.diskCount = this::countDisks;
    registry.register(MetricRegistry.name(ClusterMap.class, "datacenterCount"), datacenterCount);
    registry.register(MetricRegistry.name(ClusterMap.class, "dataNodeCount"), dataNodeCount);
    registry.register(MetricRegistry.name(ClusterMap.class, "diskCount"), diskCount);

    this.dataNodesHardUpCount = () -> countDataNodesInHardState(HardwareState.AVAILABLE);
    this.dataNodesHardDownCount = () -> countDataNodesInHardState(HardwareState.UNAVAILABLE);
    this.dataNodesUnavailableCount = this::countUnavailableDataNodes;
    this.disksHardUpCount = () -> countDisksInHardState(HardwareState.AVAILABLE);
    this.disksHardDownCount = () -> countDisksInHardState(HardwareState.UNAVAILABLE);
    this.disksUnavailableCount = this::countUnavailableDisks;
    registry.register(MetricRegistry.name(ClusterMap.class, "dataNodesHardUpCount"), dataNodesHardUpCount);
    registry.register(MetricRegistry.name(ClusterMap.class, "dataNodesHardDownCount"), dataNodesHardDownCount);
    registry.register(MetricRegistry.name(ClusterMap.class, "dataNodesUnavailableCount"), dataNodesUnavailableCount);
    registry.register(MetricRegistry.name(ClusterMap.class, "disksHardUpCount"), disksHardUpCount);
    registry.register(MetricRegistry.name(ClusterMap.class, "disksHardDownCount"), disksHardDownCount);
    registry.register(MetricRegistry.name(ClusterMap.class, "disksUnavailableCount"), disksUnavailableCount);

    // Metrics based on PartitionLayout

    this.partitionCount = this::countPartitions;
    this.partitionsReadWrite = () -> countPartitionsInState(PartitionState.READ_WRITE);
    this.partitionsReadOnly = () -> countPartitionsInState(PartitionState.READ_ONLY);
    registry.register(MetricRegistry.name(ClusterMap.class, "numberOfPartitions"), partitionCount);
    registry.register(MetricRegistry.name(ClusterMap.class, "numberOfReadWritePartitions"), partitionsReadWrite);
    registry.register(MetricRegistry.name(ClusterMap.class, "numberOfReadOnlyPartitions"), partitionsReadOnly);

    this.isMajorityReplicasDown = this::isMajorityOfReplicasDown;
    registry.register(MetricRegistry.name(ClusterMap.class, "isMajorityReplicasDown"), isMajorityReplicasDown);

    this.rawCapacityInBytes = this::getRawCapacity;
    this.allocatedRawCapacityInBytes = this::getAllocatedRawCapacity;
    this.allocatedUsableCapacityInBytes = this::getAllocatedUsableCapacity;
    registry.register(MetricRegistry.name(ClusterMap.class, "rawCapacityInBytes"), rawCapacityInBytes);
    registry.register(MetricRegistry.name(ClusterMap.class, "allocatedRawCapacityInBytes"),
        allocatedRawCapacityInBytes);
    registry.register(MetricRegistry.name(ClusterMap.class, "allocatedUsableCapacityInBytes"),
        allocatedUsableCapacityInBytes);

    dataNodeStateList = new ArrayList<Gauge<Long>>();
    diskStateList = new ArrayList<Gauge<Long>>();

    for (Datacenter datacenter : hardwareLayout.getDatacenters()) {
      for (DataNode dataNode : datacenter.getDataNodes()) {
        addDataNodeToStateMetrics(dataNode);
        for (Disk disk : dataNode.getDisks()) {
          addDiskToStateMetrics(disk);
        }
      }
    }
  }

  private void addDataNodeToStateMetrics(final DataNode dataNode) {
    final String metricName = dataNode.getHostname() + "-" + dataNode.getPort() + "-ResourceState";
    Gauge<Long> dataNodeState = () -> dataNode.getState() == HardwareState.AVAILABLE ? 1L : 0L;
    registry.register(MetricRegistry.name(ClusterMap.class, metricName), dataNodeState);
    dataNodeStateList.add(dataNodeState);
  }

  private void addDiskToStateMetrics(final Disk disk) {
    final String metricName =
        disk.getDataNode().getHostname() + "-" + disk.getDataNode().getPort() + "-" + disk.getMountPath()
            + "-ResourceState";
    Gauge<Long> diskState = () -> disk.getState() == HardwareState.AVAILABLE ? 1L : 0L;
    registry.register(MetricRegistry.name(ClusterMap.class, metricName), diskState);
    dataNodeStateList.add(diskState);
  }

  private boolean isMajorityOfReplicasDown() {
    boolean isMajorityReplicasDown = false;
    for (PartitionId partition : partitionLayout.getPartitions(null)) {
      List<? extends ReplicaId> replicas = partition.getReplicaIds();
      int replicaCount = replicas.size();
      int downReplicas = 0;
      for (ReplicaId replicaId : replicas) {
        if (replicaId.isDown()) {
          downReplicas++;
        }
      }
      if (downReplicas > replicaCount / 2) {
        isMajorityReplicasDown = true;
        break;
      }
    }
    return isMajorityReplicasDown;
  }

  private long getHardwareLayoutVersion() {
    return hardwareLayout.getVersion();
  }

  private long getPartitionLayoutVersion() {
    return partitionLayout.getVersion();
  }

  private long countDatacenters() {
    return hardwareLayout.getDatacenterCount();
  }

  private long countDataNodes() {
    return hardwareLayout.getDataNodeCount();
  }

  private long countDisks() {
    return hardwareLayout.getDiskCount();
  }

  private long countDataNodesInHardState(HardwareState hardwareState) {
    return hardwareLayout.getDataNodeInHardStateCount(hardwareState);
  }

  private long countUnavailableDataNodes() {
    return hardwareLayout.calculateUnavailableDataNodeCount();
  }

  private long countDisksInHardState(HardwareState hardwareState) {
    return hardwareLayout.getDiskInHardStateCount(hardwareState);
  }

  private long countUnavailableDisks() {
    return hardwareLayout.calculateUnavailableDiskCount();
  }

  private long countPartitions() {
    return partitionLayout.getPartitionCount();
  }

  private long countPartitionsInState(PartitionState partitionState) {
    return partitionLayout.getPartitionInStateCount(partitionState);
  }

  private long getRawCapacity() {
    return hardwareLayout.getRawCapacityInBytes();
  }

  private long getAllocatedRawCapacity() {
    return partitionLayout.getAllocatedRawCapacityInBytes();
  }

  private long getAllocatedUsableCapacity() {
    return partitionLayout.getAllocatedUsableCapacityInBytes();
  }
}
