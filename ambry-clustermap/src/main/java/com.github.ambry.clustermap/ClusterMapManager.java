package com.github.ambry.clustermap;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.github.ambry.utils.Utils.readStringFromFile;
import static com.github.ambry.utils.Utils.writeJsonToFile;

/**
 * ClusterMapManager allows components in Ambry to query the topology. This covers the {@link HardwareLayout} and the
 * {@link PartitionLayout}.
 */
public class ClusterMapManager implements ClusterMap {
  protected HardwareLayout hardwareLayout;
  protected PartitionLayout partitionLayout;

  private Logger logger = LoggerFactory.getLogger(getClass());

  public ClusterMapManager(PartitionLayout partitionLayout) {
    this.hardwareLayout = partitionLayout.getHardwareLayout();
    this.partitionLayout = partitionLayout;
  }

  public ClusterMapManager(String hardwareLayoutPath, String partitionLayoutPath) throws IOException, JSONException {
    this.hardwareLayout = new HardwareLayout(new JSONObject(readStringFromFile(hardwareLayoutPath)));
    this.partitionLayout = new PartitionLayout(hardwareLayout, new JSONObject(readStringFromFile(partitionLayoutPath)));
  }

  public void persist(String hardwareLayoutPath, String partitionLayoutPath) throws IOException, JSONException {
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


  // Administrative API
  // -----------------------

  public long getRawCapacityInBytes() {
    return hardwareLayout.getCapacityInBytes();
  }

  public long getAllocatedCapacityInBytes() {
    return partitionLayout.getCapacityInBytes();
  }

  public long getAllocatedCapacityInBytes(Datacenter datacenter) {
    long allocatedCapacityInBytes = 0;
    for (Partition partition : partitionLayout.getPartitions()) {
      for (Replica replica : partition.getReplicas()) {
        Disk disk = (Disk)replica.getDiskId();
        if (disk.getDataNode().getDatacenter().equals(datacenter)) {
          allocatedCapacityInBytes += replica.getCapacityInBytes();
        }
      }
    }
    return allocatedCapacityInBytes;
  }

  public long getAllocatedCapacityInBytes(DataNodeId dataNode) {
    long allocatedCapacityInBytes = 0;
    for (Partition partition : partitionLayout.getPartitions()) {
      for (Replica replica : partition.getReplicas()) {
        Disk disk = (Disk)replica.getDiskId();
        if (disk.getDataNode().equals(dataNode)) {
          allocatedCapacityInBytes += replica.getCapacityInBytes();
        }
      }
    }
    return allocatedCapacityInBytes;
  }

  public long getAllocatedCapacityInBytes(Disk disk) {
    long allocatedCapacityInBytes = 0;
    for (Partition partition : partitionLayout.getPartitions()) {
      for (Replica replica : partition.getReplicas()) {
        Disk currentDisk = (Disk)replica.getDiskId();
        if (currentDisk.equals(disk)) {
          allocatedCapacityInBytes += replica.getCapacityInBytes();
        }
      }
    }
    return allocatedCapacityInBytes;
  }

  public long getFreeCapacityInBytes() {
    return getRawCapacityInBytes() - getAllocatedCapacityInBytes();
  }

  public long getFreeCapacityInBytes(Datacenter datacenter) {
    return datacenter.getCapacityInBytes() - getAllocatedCapacityInBytes(datacenter);
  }

  public long getFreeCapacityInBytes(DataNode dataNode) {
    return dataNode.getCapacityInBytes() - getAllocatedCapacityInBytes(dataNode);
  }

  public long getFreeCapacityInBytes(Disk disk) {
    return disk.getCapacityInBytes() - getAllocatedCapacityInBytes(disk);
  }

  public PartitionId addNewPartition(List<Disk> disks, long replicaCapacityInBytes) {
    return partitionLayout.addNewPartition(disks, replicaCapacityInBytes);
  }

  // Determine if there is enough capacity to allocate a PartitionId.
  private boolean enoughFreeCapacity(int replicaCountPerDatacenter, long replicaCapacityInBytes) {
    for (Datacenter datacenter : hardwareLayout.getDatacenters()) {
      if (getFreeCapacityInBytes(datacenter) < replicaCountPerDatacenter * replicaCapacityInBytes) {
        logger.warn("Insufficient free space in datacenter {} ({} bytes free)", datacenter.getName(),
                    getFreeCapacityInBytes(datacenter));
        return false;
      }

      int rcpd = replicaCountPerDatacenter;
      for (DataNode dataNode : datacenter.getDataNodes()) {
        for (Disk disk : dataNode.getDisks()) {
          if (getFreeCapacityInBytes(disk) >= replicaCapacityInBytes) {
            rcpd--;
            break; // Only one replica per DataNodeId.
          }
        }
      }
      if (rcpd > 0) {
        logger.warn("Insufficient DataNodes ({}) with free space in datacenter {} for {} Replicas)", rcpd,
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
      List<DataNode> shuffledDataNodes = new ArrayList<DataNode>(datacenter.getDataNodes());
      Collections.shuffle(shuffledDataNodes);

      int rcpd = replicaCountPerDatacenter;
      for (DataNode dataNode : shuffledDataNodes) {
        List<Disk> shuffledDisks = new ArrayList<Disk>(dataNode.getDisks());
        Collections.shuffle(shuffledDisks);

        for (Disk disk : shuffledDisks) {
          if (getFreeCapacityInBytes(disk) >= replicaCapacityInBytes) {
            allocatedDisks.add(disk);
            rcpd--;
            break; // Only one replica per DataNodeId.
          }
        }

        if (rcpd == 0) {
          break;
        }
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

    while (enoughFreeCapacity(replicaCountPerDatacenter, replicaCapacityInBytes) && numPartitions > 0) {
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
