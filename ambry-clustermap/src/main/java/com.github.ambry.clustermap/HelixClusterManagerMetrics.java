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


/**
 * Metrics for the {@link HelixClusterManager}
 */
class HelixClusterManagerMetrics {
  private final HelixClusterManager.HelixClusterManagerCallback clusterMapCallback;
  private final MetricRegistry registry;

  public final Counter liveInstanceChangeTriggerCount;
  public final Counter externalViewChangeTriggerCount;
  public final Counter instanceConfigChangeTriggerCount;
  public final Counter getPartitionIdFromStreamMismatchCount;
  public final Counter getWritablePartitionIdsMismatchCount;
  public final Counter getAllPartitionIdsMismatchCount;
  public final Counter hasDatacenterMismatchCount;
  public final Counter getDataNodeIdMismatchCount;
  public final Counter getReplicaIdsMismatchCount;
  public final Counter getDataNodeIdsMismatchCount;

  public Gauge<Long> helixClusterManagerInstantiationFailed;

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
    externalViewChangeTriggerCount =
        registry.counter(MetricRegistry.name(HelixClusterManager.class, "externalViewChangeTriggerCount"));
    instanceConfigChangeTriggerCount =
        registry.counter(MetricRegistry.name(HelixClusterManager.class, "instanceConfigChangeTriggerCount"));
    getPartitionIdFromStreamMismatchCount =
        registry.counter(MetricRegistry.name(HelixClusterManager.class, "getPartitionIdFromStreamMismatchCount"));
    getWritablePartitionIdsMismatchCount =
        registry.counter(MetricRegistry.name(HelixClusterManager.class, "getWritablePartitionIdsMismatchCount"));
    getAllPartitionIdsMismatchCount =
        registry.counter(MetricRegistry.name(HelixClusterManager.class, "getAllPartitionIdsMismatchCount"));
    hasDatacenterMismatchCount =
        registry.counter(MetricRegistry.name(HelixClusterManager.class, "hasDatacenterMismatchCount"));
    getDataNodeIdMismatchCount =
        registry.counter(MetricRegistry.name(HelixClusterManager.class, "getDataNodeIdMismatchCount"));
    getReplicaIdsMismatchCount =
        registry.counter(MetricRegistry.name(HelixClusterManager.class, "getReplicaIdsMismatchCount"));
    getDataNodeIdsMismatchCount =
        registry.counter(MetricRegistry.name(HelixClusterManager.class, "getDataNodeIdsMismatchCount"));
  }

  void initializeInstantiationMetric(final boolean instantiated) {
    helixClusterManagerInstantiationFailed = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return instantiated ? 0L : 1L;
      }
    };
    registry.register(MetricRegistry.name(HelixClusterManager.class, "instantiationFailed"),
        helixClusterManagerInstantiationFailed);
  }

  /**
   * Initialize datacenter related metrics.
   */
  void initializeDatacenterMetrics() {
    Gauge<Long> datacenterCount = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return clusterMapCallback.getDatacenterCount();
      }
    };
    registry.register(MetricRegistry.name(HelixClusterManager.class, "datacenterCount"), datacenterCount);
  }

  /**
   * Initialize datanode related metrics.
   */
  void initializeDataNodeMetrics() {
    Gauge<Long> dataNodeCount = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return clusterMapCallback.getDatanodeCount();
      }
    };
    registry.register(MetricRegistry.name(HelixClusterManager.class, "dataNodeCount"), dataNodeCount);

    Gauge<Long> dataNodeDownCount = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return clusterMapCallback.getDownDatanodesCount();
      }
    };
    registry.register(MetricRegistry.name(HelixClusterManager.class, "dataNodeDownCount"), dataNodeDownCount);

    for (final AmbryDataNode datanode : clusterMapCallback.getDatanodes()) {
      final String metricName = datanode.getHostname() + "-" + datanode.getPort() + "-DataNodeResourceState";
      Gauge<Long> dataNodeState = new Gauge<Long>() {
        @Override
        public Long getValue() {
          return datanode.getState() == HardwareState.AVAILABLE ? 1L : 0L;
        }
      };
      registry.register(MetricRegistry.name(HelixClusterManager.class, metricName), dataNodeState);
    }
  }

  /**
   * Initialize disk related metrics.
   */
  void initializeDiskMetrics() {
    Gauge<Long> diskCount = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return clusterMapCallback.getDiskCount();
      }
    };
    registry.register(MetricRegistry.name(HelixClusterManager.class, "diskCount"), diskCount);

    Gauge<Long> diskDownCount = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return clusterMapCallback.getDownDisksCount();
      }
    };
    registry.register(MetricRegistry.name(HelixClusterManager.class, "diskDownCount"), diskDownCount);

    for (final AmbryDisk disk : clusterMapCallback.getDisks()) {
      final String metricName =
          disk.getDataNode().getHostname() + "-" + disk.getDataNode().getPort() + "-" + disk.getMountPath()
              + "-DiskResourceState";
      Gauge<Long> diskState = new Gauge<Long>() {
        @Override
        public Long getValue() {
          return disk.getState() == HardwareState.AVAILABLE ? 1L : 0L;
        }
      };
      registry.register(MetricRegistry.name(HelixClusterManager.class, metricName), diskState);
    }
  }

  /**
   * Initialize partition related metrics.
   */
  void initializePartitionMetrics() {
    Gauge<Long> partitionCount = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return clusterMapCallback.getPartitionCount();
      }
    };
    registry.register(MetricRegistry.name(HelixClusterManager.class, "partitionCount"), partitionCount);

    Gauge<Long> partitionReadWriteCount = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return clusterMapCallback.getPartitionReadWriteCount();
      }
    };
    registry.register(MetricRegistry.name(HelixClusterManager.class, "partitionReadWriteCount"),
        partitionReadWriteCount);

    Gauge<Long> partitionSealedCount = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return clusterMapCallback.getPartitionSealedCount();
      }
    };
    registry.register(MetricRegistry.name(HelixClusterManager.class, "partitionSealedCount"), partitionSealedCount);

    Gauge<Long> isMajorityReplicasDownForAnyPartition = new Gauge<Long>() {
      @Override
      public Long getValue() {
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
      }
    };
    registry.register(MetricRegistry.name(HelixClusterManager.class, "isMajorityReplicasDownForAnyPartition"),
        isMajorityReplicasDownForAnyPartition);
  }

  /**
   * Initialize capacity related metrics.
   */
  void initializeCapacityMetrics() {
    Gauge<Long> rawTotalCapacityInBytes = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return clusterMapCallback.getRawCapacity();
      }
    };
    registry.register(MetricRegistry.name(HelixClusterManager.class, "rawTotalCapacityBytes"), rawTotalCapacityInBytes);
    Gauge<Long> allocatedRawCapacityInBytes = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return clusterMapCallback.getAllocatedRawCapacity();
      }
    };
    registry.register(MetricRegistry.name(HelixClusterManager.class, "allocatedRawCapacityBytes"),
        allocatedRawCapacityInBytes);

    Gauge<Long> allocatedUsableCapacityInBytes = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return clusterMapCallback.getAllocatedUsableCapacity();
      }
    };
    registry.register(MetricRegistry.name(HelixClusterManager.class, "allocatedUsableCapacityBytes"),
        allocatedUsableCapacityInBytes);
  }
}
