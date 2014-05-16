package com.github.ambry.clustermap;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

/**
 * Metrics for ClusterMap (HardwareLayout & PartitionLayout)
 */
public class ClusterMapMetrics {
  private final HardwareLayout hardwareLayout;
  private final PartitionLayout partitionLayout;

  public final Gauge<Long> hardwareLayoutVersion;
  public final Gauge<Long> partitionLayoutVersion;
  
  public final Gauge<Long> datacenterCount;
  public final Gauge<Long> dataNodeCount;
  public final Gauge<Long> diskCount;
  public final Gauge<Long> dataNodesHardUpCount;
  public final Gauge<Long> dataNodesHardDownCount;
  public final Gauge<Long> dataNodesSoftDownCount;
  public final Gauge<Long> disksHardUpCount;
  public final Gauge<Long> disksHardDownCount;
  public final Gauge<Long> disksSoftDownCount;

  public final Gauge<Long> partitionCount;
  public final Gauge<Long> partitionsReadWrite;
  public final Gauge<Long> partitionsReadOnly;

  public final Gauge<Long> rawCapacityInBytes;
  public final Gauge<Long> allocatedRawCapacityInBytes;
  public final Gauge<Long> allocatedUsableCapacityInBytes;

  public ClusterMapMetrics(HardwareLayout hardwareLayout,
                           PartitionLayout partitionLayout,
                           MetricRegistry registry) {
    this.hardwareLayout = hardwareLayout;
    this.partitionLayout = partitionLayout;

    // Metrics based on HardwareLayout

    this.hardwareLayoutVersion = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return getHardwareLayoutVersion();
      }
    };
    this.partitionLayoutVersion = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return getPartitionLayoutVersion();
      }
    };
    registry.register(MetricRegistry.name(ClusterMap.class, "hardwareLayoutVersion"), hardwareLayoutVersion);
    registry.register(MetricRegistry.name(ClusterMap.class, "partitionLayoutVersion"), partitionLayoutVersion);

    this.datacenterCount = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return countDatacenters();
      }
    };
    this.dataNodeCount = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return countDataNodes();
      }
    };
    this.diskCount = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return countDisks();
      }
    };
    registry.register(MetricRegistry.name(ClusterMap.class, "datacenterCount"), datacenterCount);
    registry.register(MetricRegistry.name(ClusterMap.class, "dataNodeCount"), dataNodeCount);
    registry.register(MetricRegistry.name(ClusterMap.class, "diskCount"), diskCount);

    this.dataNodesHardUpCount = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return countDataNodesInHardState(HardwareState.AVAILABLE);
      }
    };
    this.dataNodesHardDownCount = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return countDataNodesInHardState(HardwareState.UNAVAILABLE);
      }
    };
    this.dataNodesSoftDownCount = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return countSoftDownDataNodes();
      }
    };
    this.disksHardUpCount = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return countDisksInHardState(HardwareState.AVAILABLE);
      }
    };
    this.disksHardDownCount = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return countDisksInHardState(HardwareState.UNAVAILABLE);
      }
    };
    this.disksSoftDownCount = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return countSoftDownDisks();
      }
    };
    registry.register(MetricRegistry.name(ClusterMap.class, "dataNodesHardUpCount"), dataNodesHardUpCount);
    registry.register(MetricRegistry.name(ClusterMap.class, "dataNodesHardDownCount"), dataNodesHardDownCount);
    registry.register(MetricRegistry.name(ClusterMap.class, "dataNodesSoftDownCount"), dataNodesSoftDownCount);
    registry.register(MetricRegistry.name(ClusterMap.class, "disksHardUpCount"), disksHardUpCount);
    registry.register(MetricRegistry.name(ClusterMap.class, "disksHardDownCount"), disksHardDownCount);
    registry.register(MetricRegistry.name(ClusterMap.class, "disksSoftDownCount"), disksSoftDownCount);

    // Metrics based on PartitionLayout

    this.partitionCount = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return countPartitions();
      }
    };
    this.partitionsReadWrite = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return countPartitionsInState(PartitionState.READ_WRITE);
      }
    };
    this.partitionsReadOnly = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return countPartitionsInState(PartitionState.READ_ONLY);
      }
    };
    registry.register(MetricRegistry.name(ClusterMap.class, "numberOfPartitions"), partitionCount);
    registry.register(MetricRegistry.name(ClusterMap.class, "numberOfReadWritePartitions"), partitionsReadWrite);
    registry.register(MetricRegistry.name(ClusterMap.class, "numberOfReadOnlyPartitions"), partitionsReadOnly);

    this.rawCapacityInBytes = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return getRawCapacity();
      }
    };
    this.allocatedRawCapacityInBytes = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return getAllocatedRawCapacity();
      }
    };
    this.allocatedUsableCapacityInBytes = new Gauge<Long>() {
      @Override
      public Long getValue() {
        return getAllocatedUsableCapacity();
      }
    };
    registry.register(MetricRegistry.name(ClusterMap.class, "rawCapacityInBytes"), rawCapacityInBytes);
    registry.register(MetricRegistry.name(ClusterMap.class, "allocatedRawCapacityInBytes"), allocatedRawCapacityInBytes);
    registry.register(MetricRegistry.name(ClusterMap.class, "allocatedUsableCapacityInBytes"), allocatedUsableCapacityInBytes);
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

  private long countSoftDownDataNodes() {
    return hardwareLayout.calculateSoftDownDataNodeCount();
  }

  private long countDisksInHardState(HardwareState hardwareState) {
    return hardwareLayout.getDiskInHardStateCount(hardwareState);
  }

  private long countSoftDownDisks() {
    return hardwareLayout.calculateSoftDownDiskCount();
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
