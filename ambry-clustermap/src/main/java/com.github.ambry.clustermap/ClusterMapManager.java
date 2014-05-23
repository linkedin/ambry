package com.github.ambry.clustermap;

import com.codahale.metrics.MetricRegistry;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;

import static com.github.ambry.utils.Utils.readStringFromFile;
import static com.github.ambry.utils.Utils.writeJsonToFile;

/**
 * ClusterMapManager allows components in Ambry to query the topology. This covers the {@link HardwareLayout} and the
 * {@link PartitionLayout}.
 */
public class ClusterMapManager implements ClusterMap {
  protected final HardwareLayout hardwareLayout;
  protected final PartitionLayout partitionLayout;
  private final MetricRegistry metricRegistry;
  private final ClusterMapMetrics clusterMapMetrics;

  private Logger logger = LoggerFactory.getLogger(getClass());

  public ClusterMapManager(PartitionLayout partitionLayout) {
    if (logger.isTraceEnabled())
      logger.trace("ClusterMapManager " + partitionLayout);
    this.hardwareLayout = partitionLayout.getHardwareLayout();
    this.partitionLayout = partitionLayout;
    this.metricRegistry = new MetricRegistry();
    this.clusterMapMetrics = new ClusterMapMetrics(this.hardwareLayout, this.partitionLayout, this.metricRegistry);
  }

  public ClusterMapManager(String hardwareLayoutPath,
                           String partitionLayoutPath) throws IOException, JSONException {
    logger.trace("ClusterMapManager " + hardwareLayoutPath + ", " + partitionLayoutPath);
    this.hardwareLayout = new HardwareLayout(new JSONObject(readStringFromFile(hardwareLayoutPath)));
    this.partitionLayout = new PartitionLayout(hardwareLayout, new JSONObject(readStringFromFile(partitionLayoutPath)));
    this.metricRegistry = new MetricRegistry();
    this.clusterMapMetrics = new ClusterMapMetrics(this.hardwareLayout, this.partitionLayout, this.metricRegistry);
  }

  public void persist(String hardwareLayoutPath, String partitionLayoutPath) throws IOException, JSONException {
    logger.trace("persist " + hardwareLayoutPath + ", " + partitionLayoutPath);
    writeJsonToFile(hardwareLayout.toJSONObject(), hardwareLayoutPath);
    writeJsonToFile(partitionLayout.toJSONObject(), partitionLayoutPath);
  }

  // Implementation of ClusterMap interface
  // --------------------------------------

  public List<? extends PartitionId> getWritablePartitionIds() {
    return partitionLayout.getWritablePartitions();
  }

  @Override
  public PartitionId getPartitionIdFromStream(DataInputStream stream) throws IOException {
    return partitionLayout.getPartition(stream);
  }

  @Override
  public long getWritablePartitionIdsCount() {
    return getWritablePartitionIds().size();
  }

  @Override
  public PartitionId getWritablePartitionIdAt(long index) {
    return partitionLayout.getWritablePartitions().get((int)index);
  }

  @Override
  public boolean hasDatacenter(String datacenterName) {
    return hardwareLayout.findDatacenter(datacenterName) != null;
  }

  @Override
  public DataNodeId getDataNodeId(String hostname, int port) {
    return hardwareLayout.findDataNode(hostname, port);
  }

  @Override
  public List<ReplicaId> getReplicaIds(DataNodeId dataNodeId) {
    List<Replica> replicas = getReplicas(dataNodeId);
    return new ArrayList<ReplicaId>(replicas);
  }

  public List<Replica> getReplicas(DataNodeId dataNodeId) {
    List<Replica> replicas = new ArrayList<Replica>();
    for (Partition partition : partitionLayout.getPartitions()) {
      for (Replica replica : partition.getReplicas()) {
        if (replica.getDataNodeId().equals(dataNodeId)) {
          replicas.add(replica);
        }
      }
    }
    return replicas;
  }

  @Override
  public List<DataNodeId> getDataNodeIds() {
    List<DataNodeId> dataNodeIds = new ArrayList<DataNodeId>();
    for (Datacenter datacenter : hardwareLayout.getDatacenters()) {
        dataNodeIds.addAll(datacenter.getDataNodes());
    }
    return dataNodeIds;
  }

  @Override
  public MetricRegistry getMetricRegistry() {
    return metricRegistry;
  }

  // Administrative API
  // -----------------------

  public long getRawCapacityInBytes() {
    return hardwareLayout.getRawCapacityInBytes();
  }

  public long getAllocatedRawCapacityInBytes() {
    return partitionLayout.getAllocatedRawCapacityInBytes();
  }

  public long getAllocatedUsableCapacityInBytes() {
    return partitionLayout.getAllocatedUsableCapacityInBytes();
  }

  public long getAllocatedRawCapacityInBytes(Datacenter datacenter) {
    long allocatedRawCapacityInBytes = 0;
    for (Partition partition : partitionLayout.getPartitions()) {
      for (Replica replica : partition.getReplicas()) {
        Disk disk = (Disk)replica.getDiskId();
        if (disk.getDataNode().getDatacenter().equals(datacenter)) {
          allocatedRawCapacityInBytes += replica.getCapacityInBytes();
        }
      }
    }
    return allocatedRawCapacityInBytes;
  }

  public long getAllocatedRawCapacityInBytes(DataNodeId dataNode) {
    long allocatedRawCapacityInBytes = 0;
    for (Partition partition : partitionLayout.getPartitions()) {
      for (Replica replica : partition.getReplicas()) {
        Disk disk = (Disk)replica.getDiskId();
        if (disk.getDataNode().equals(dataNode)) {
          allocatedRawCapacityInBytes += replica.getCapacityInBytes();
        }
      }
    }
    return allocatedRawCapacityInBytes;
  }

  public long getAllocatedRawCapacityInBytes(Disk disk) {
    long allocatedRawCapacityInBytes = 0;
    for (Partition partition : partitionLayout.getPartitions()) {
      for (Replica replica : partition.getReplicas()) {
        Disk currentDisk = (Disk)replica.getDiskId();
        if (currentDisk.equals(disk)) {
          allocatedRawCapacityInBytes += replica.getCapacityInBytes();
        }
      }
    }
    return allocatedRawCapacityInBytes;
  }

  public long getUnallocatedRawCapacityInBytes() {
    return getRawCapacityInBytes() - getAllocatedRawCapacityInBytes();
  }

  public long getUnallocatedRawCapacityInBytes(Datacenter datacenter) {
    return datacenter.getRawCapacityInBytes() - getAllocatedRawCapacityInBytes(datacenter);
  }

  public long getUnallocatedRawCapacityInBytes(DataNode dataNode) {
    return dataNode.getRawCapacityInBytes() - getAllocatedRawCapacityInBytes(dataNode);
  }

  public long getUnallocatedRawCapacityInBytes(Disk disk) {
    return disk.getRawCapacityInBytes() - getAllocatedRawCapacityInBytes(disk);
  }

  public DataNode getDataNodeWithMostUnallocatedRawCapacity(Datacenter dc, Set nodesToExclude) {
    DataNode maxCapacityNode = null;
    List<DataNode> dataNodes = dc.getDataNodes();
    for (DataNode dataNode : dataNodes) {
      if (!nodesToExclude.contains(dataNode) &&
          (maxCapacityNode == null || getUnallocatedRawCapacityInBytes(dataNode) > getUnallocatedRawCapacityInBytes(
                  maxCapacityNode))) {
        maxCapacityNode = dataNode;
      }
    }
    return maxCapacityNode;
  }

  public Disk getDiskWithMostUnallocatedRawCapacity(DataNode node, long minCapacity) {
    Disk maxCapacityDisk = null;
    List<Disk> disks = node.getDisks();
    for (Disk disk : disks) {
      if ((maxCapacityDisk == null || getUnallocatedRawCapacityInBytes(disk) > getUnallocatedRawCapacityInBytes(
              maxCapacityDisk)) &&
          getUnallocatedRawCapacityInBytes(disk) >= minCapacity) {
        maxCapacityDisk = disk;
      }
    }
    return maxCapacityDisk;
  }

  public PartitionId addNewPartition(List<Disk> disks, long replicaCapacityInBytes) {
    return partitionLayout.addNewPartition(disks, replicaCapacityInBytes);
  }

  // Determine if there is enough capacity to allocate a PartitionId.
  private boolean checkEnoughUnallocatedRawCapacity(int replicaCountPerDatacenter, long replicaCapacityInBytes) {
    for (Datacenter datacenter : hardwareLayout.getDatacenters()) {
      if (getUnallocatedRawCapacityInBytes(datacenter) < replicaCountPerDatacenter * replicaCapacityInBytes) {
        logger.warn("Insufficient unallocated space in datacenter {} ({} bytes unallocated)", datacenter.getName(),
                    getUnallocatedRawCapacityInBytes(datacenter));
        return false;
      }

      int rcpd = replicaCountPerDatacenter;
      for (DataNode dataNode : datacenter.getDataNodes()) {
        for (Disk disk : dataNode.getDisks()) {
          if (getUnallocatedRawCapacityInBytes(disk) >= replicaCapacityInBytes) {
            rcpd--;
            break; // Only one replica per DataNodeId.
          }
        }
      }
      if (rcpd > 0) {
        logger.warn("Insufficient DataNodes ({}) with unallocated space in datacenter {} for {} Replicas)", rcpd,
                    datacenter.getName(), replicaCountPerDatacenter);
        return false;
      }
    }

    return true;
  }

  // Allocate unique datanode.disks for each replica in each datacenter up to replicaCountPerDatacenter (hard-code all
  // datacenters to have same number of replicas for now).
  private List<Disk> allocateDisksForPartition(int replicaCountPerDatacenter, long replicaCapacityInBytes) {
    ArrayList<Disk> allocatedDisks = new ArrayList<Disk>();

    for (Datacenter datacenter : hardwareLayout.getDatacenters()) {
      int rcpd = replicaCountPerDatacenter;
      Set<DataNode> nodesToExclude = new HashSet<DataNode>();
      for (int i = 0; i < rcpd; i++) {
        DataNode nodeWithMostCapacity = getDataNodeWithMostUnallocatedRawCapacity(datacenter, nodesToExclude);
        Disk diskWithMostCapacity = getDiskWithMostUnallocatedRawCapacity(nodeWithMostCapacity, replicaCapacityInBytes);
        allocatedDisks.add(diskWithMostCapacity);
        nodesToExclude.add(nodeWithMostCapacity);
      }
    }
    return allocatedDisks;
  }

  // Best effort (or less) allocation of partitions. I.e., size of returned list may be less than numPartitions.
  // Hackish 1st attempt at PartitionId allocation policy to confirm Administrative API is sufficient. All cluster map
  // operations are performed by a single thread in some tool.
  public List<PartitionId> allocatePartitions(int numPartitions,
                                              int replicaCountPerDatacenter,
                                              long replicaCapacityInBytes) {
    ArrayList<PartitionId> partitions = new ArrayList<PartitionId>(numPartitions);

    while (checkEnoughUnallocatedRawCapacity(replicaCountPerDatacenter, replicaCapacityInBytes) && numPartitions > 0) {
      List<Disk> disks = allocateDisksForPartition(replicaCountPerDatacenter, replicaCapacityInBytes);
      if (disks.size() == 0) {
        System.err.println("numPartitions: " + numPartitions);
        break;
      }
      partitions.add(partitionLayout.addNewPartition(disks, replicaCapacityInBytes));
      numPartitions--;
    }

    return partitions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ClusterMapManager that = (ClusterMapManager)o;

    if (hardwareLayout != null ? !hardwareLayout.equals(that.hardwareLayout) : that.hardwareLayout != null)
      return false;
    return !(partitionLayout != null ? !partitionLayout.equals(that.partitionLayout) : that.partitionLayout != null);

  }
}
