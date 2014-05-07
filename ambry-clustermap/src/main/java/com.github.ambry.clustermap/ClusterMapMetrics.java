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
  public final Gauge<Long> disksHardUpCount;
  public final Gauge<Long> disksHardDownCount;

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

    this.hardwareLayoutVersion = new com.codahale.metrics.Gauge<Long>() {
      @Override
      public Long getValue() {
        return getHardwareLayoutVersion();
      }
    };
    this.partitionLayoutVersion = new com.codahale.metrics.Gauge<Long>() {
      @Override
      public Long getValue() {
        return getPartitionLayoutVersion();
      }
    };
    registry.register("hardwareLayoutVersion", hardwareLayoutVersion);
    registry.register("partitionLayoutVersion", partitionLayoutVersion);

    this.datacenterCount = new com.codahale.metrics.Gauge<Long>() {
      @Override
      public Long getValue() {
        return countDatacenters();
      }
    };
    this.dataNodeCount = new com.codahale.metrics.Gauge<Long>() {
      @Override
      public Long getValue() {
        return countDataNodes();
      }
    };
    this.diskCount = new com.codahale.metrics.Gauge<Long>() {
      @Override
      public Long getValue() {
        return countDisks();
      }
    };
    registry.register("datacenterCount", datacenterCount);
    registry.register("dataNodeCount", dataNodeCount);
    registry.register("diskCount", diskCount);

    this.dataNodesHardUpCount = new com.codahale.metrics.Gauge<Long>() {
      @Override
      public Long getValue() {
        return countDataNodesInState(HardwareState.AVAILABLE);
      }
    };
    this.dataNodesHardDownCount = new com.codahale.metrics.Gauge<Long>() {
      @Override
      public Long getValue() {
        return countDataNodesInState(HardwareState.UNAVAILABLE);
      }
    };
    this.disksHardUpCount = new com.codahale.metrics.Gauge<Long>() {
      @Override
      public Long getValue() {
        return countDisksInState(HardwareState.AVAILABLE);
      }
    };
    this.disksHardDownCount = new com.codahale.metrics.Gauge<Long>() {
      @Override
      public Long getValue() {
        return countDisksInState(HardwareState.UNAVAILABLE);
      }
    };
    registry.register("dataNodesHardUpCount", dataNodesHardUpCount);
    registry.register("dataNodesHardDownCount", dataNodesHardDownCount);
    registry.register("disksHardUpCount", disksHardUpCount);
    registry.register("disksHardDownCount", disksHardDownCount);

    this.partitionCount = new com.codahale.metrics.Gauge<Long>() {
      @Override
      public Long getValue() {
        return countPartitions();
      }
    };
    this.partitionsReadWrite = new com.codahale.metrics.Gauge<Long>() {
      @Override
      public Long getValue() {
        return countPartitionsInState(PartitionState.READ_WRITE);
      }
    };
    this.partitionsReadOnly = new com.codahale.metrics.Gauge<Long>() {
      @Override
      public Long getValue() {
        return countPartitionsInState(PartitionState.READ_ONLY);
      }
    };
    registry.register("numberOfPartitions", partitionCount);
    registry.register("numberOfReadWritePartitions", partitionsReadWrite);
    registry.register("numberOfReadOnlyPartitions", partitionsReadOnly);

    this.rawCapacityInBytes = new com.codahale.metrics.Gauge<Long>() {
      @Override
      public Long getValue() {
        return getRawCapacity();
      }
    };
    this.allocatedRawCapacityInBytes = new com.codahale.metrics.Gauge<Long>() {
      @Override
      public Long getValue() {
        return getAllocatedRawCapacity();
      }
    };
    this.allocatedUsableCapacityInBytes = new com.codahale.metrics.Gauge<Long>() {
      @Override
      public Long getValue() {
        return getAllocatedUsableCapacity();
      }
    };
    registry.register("rawCapacityInBytes", rawCapacityInBytes);
    registry.register("allocatedRawCapacityInBytes", allocatedRawCapacityInBytes);
    registry.register("allocatedUsableCapacityInBytes", allocatedUsableCapacityInBytes);
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

  private long countDataNodesInState(HardwareState hardwareState) {
    return hardwareLayout.getDataNodeInStateCount(hardwareState);
  }

  private long countDisksInState(HardwareState hardwareState) {
    return hardwareLayout.getDiskInStateCount(hardwareState);
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
