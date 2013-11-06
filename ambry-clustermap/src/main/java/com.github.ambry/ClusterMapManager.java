package com.github.ambry;

// TODO:
// - File SerDe and use tmp test file in unit test
// ...
// - ClusterMapAPI with javadoc
// - Add logging
// ...
// - Cleaner/simpler JSON SerDe. Currently overly verbose & ugly.
// - Add TODO file to this module for all v1 goodness.
// - AddNode && AddDisk actually working with node & disk in way SRE is OK with
// - More unit tests
// - Document JSON format


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * ClusterMapManager allows components in Ambry to query the topology. This covers both cluster hardware and data layout.
 */
public class ClusterMapManager {
  protected Cluster cluster;
  protected Layout layout;


  public ClusterMapManager(Cluster cluster, Layout layout) {
    this.cluster = cluster;
    this.layout = layout;
  }

  // TODO: File-based SerDe

  // API for the Coordinator
  // -----------------------

  public ArrayList<Partition> getReadWritePartitions() {
    ArrayList<Partition> rwPartitions= new ArrayList<Partition>();
    for(Partition partition : layout.getPartitions()) {
      if (partition.getState() == Partition.State.READ_WRITE) {
        rwPartitions.add(partition);
      }
    }
    return rwPartitions;
  }

  public Partition getPartition(PartitionId partitionId) {
    return layout.getPartition(partitionId);
  }


  // API for the DataNode
  // -----------------------

  public ArrayList<Replica> getPeerReplicas(Replica replica) {
    ArrayList<Replica> peerReplicas = new ArrayList<Replica>();

    for (Replica peerReplica : replica.getPartition().getReplicas()) {
      if(!replica.equals(peerReplica)) {
        peerReplicas.add(peerReplica);
      }
    }

    return peerReplicas;
  }

  // Administrative API
  // -----------------------

  public long getRawCapacityGB() {
    return cluster.getCapacityGB();
  }

  public long getAllocatedCapacityGB() {
    return layout.getCapacityGB();
  }

  public long getAllocatedCapacityGB(Datacenter datacenter) {
    long allocatedCapacityGB = 0;
    for(Partition partition : layout.getPartitions()) {
      for (Replica replica : partition.getReplicas()) {
        if (replica.getDisk().getDataNode().getDatacenter().equals(datacenter)) {
          allocatedCapacityGB += replica.getCapacityGB();
        }
      }
    }
    return allocatedCapacityGB;
  }

  public long getAllocatedCapacityGB(DataNode dataNode) {
    long allocatedCapacityGB = 0;
    for(Partition partition : layout.getPartitions()) {
      for (Replica replica : partition.getReplicas()) {
        if (replica.getDisk().getDataNode().equals(dataNode)) {
          allocatedCapacityGB += replica.getCapacityGB();
        }
      }
    }
    return allocatedCapacityGB;
  }

  public long getAllocatedCapacityGB(Disk disk) {
    long allocatedCapacityGB = 0;
    for(Partition partition : layout.getPartitions()) {
      for (Replica replica : partition.getReplicas()) {
        if (replica.getDisk().equals(disk)) {
          allocatedCapacityGB += replica.getCapacityGB();
        }
      }
    }
    return allocatedCapacityGB;
  }

  public long getFreeCapacityGB() {
    return getRawCapacityGB() - getAllocatedCapacityGB();
  }

  public long getFreeCapacityGB(Datacenter datacenter) {
    return datacenter.getCapacityGB() - getAllocatedCapacityGB(datacenter);
  }

  public long getFreeCapacityGB(DataNode dataNode) {
    return dataNode.getCapacityGB() - getAllocatedCapacityGB(dataNode);
  }

  public long getFreeCapacityGB(Disk disk) {
    return disk.getCapacityGB() - getAllocatedCapacityGB(disk);
  }

  public Datacenter addNewDataCenter(String datacenterName) {
    return cluster.addNewDataCenter(datacenterName);
  }

  public DataNode addNewDataNode(String datacenterName, String hostname, int port) {
    return cluster.addNewDataNode(datacenterName, hostname, port);
  }

  public Disk addNewDisk(String hostname, int port, String mountPath, long capacityGB) {
    return cluster.addNewDisk(hostname, port, mountPath, capacityGB);
  }

  public Partition addNewPartition(List<Disk> disks, long replicaCapacityGB) {
    return layout.addNewPartition(disks, replicaCapacityGB);
  }

  // TODO: Is this API needed (at this time) (at this level)?
  public Replica addNewReplicaToPartition(Partition partition, Disk disk) {
    return layout.addNewReplicaToPartition(partition, disk);
  }


  // Determine if there is enough capacity to allocate a Partition
  private boolean enoughFreeCapacity(int replicaCountPerDatacenter, long replicaCapacityGB) {
    for(Datacenter datacenter : cluster.getDatacenters()) {
      if (getFreeCapacityGB(datacenter) < replicaCountPerDatacenter * replicaCapacityGB) {
        // Log
        return false;
      }

      int rcpd = replicaCountPerDatacenter;
      for (DataNode dataNode : datacenter.getDataNodes()) {
        for (Disk disk : dataNode.getDisks()) {
          if (getFreeCapacityGB(disk) >= replicaCapacityGB) {
            rcpd--;
            break; // Only one replica per DataNode.
          }
        }
      }
      if (rcpd > 0) {
        // Log
        return false;
      }
    }

    return true;
  }

  // Allocate unique datanode.disks for each replica in each datacenter up to replicaCountPerDatacenter
  private List<Disk> allocateDisksForPartition(int replicaCountPerDatacenter, long replicaCapacityGB) {
    ArrayList<Disk> allocatedDisks = new ArrayList<Disk>();

    for(Datacenter datacenter : cluster.getDatacenters()) {
      List<DataNode> suffledDataNodes = new ArrayList<DataNode>(datacenter.getDataNodes());
      Collections.shuffle(suffledDataNodes);

      int rcpd = replicaCountPerDatacenter;
      for (DataNode dataNode : suffledDataNodes) {
        List<Disk> shuffledDisks = new ArrayList<Disk>(dataNode.getDisks());
        Collections.shuffle(shuffledDisks);

        for (Disk disk : shuffledDisks) {
          if (disk.getCapacityGB() > replicaCapacityGB) {
            allocatedDisks.add(disk);
            rcpd--;
            break; // Only one replica per DataNode.
          }
        }

        if(rcpd == 0) {
          break;
        }
      }
    }

    return allocatedDisks;
  }

  // Best effort (or less) allocation of partitions. I.e., size of returned list may be less than numPartitions.
  // Hackish 1st attempt at Partition allocation policy to confirm Administrative API is sufficient.
  public List<Partition> allocatePartitions(int numPartitions, long replicaCapacityGB) {
    ArrayList<Partition> partitions = new ArrayList<Partition>();
    final int replicaCountPerDatacenter = 2;


    while (enoughFreeCapacity(replicaCountPerDatacenter, replicaCapacityGB) && numPartitions > 0) {
      List<Disk> disks = allocateDisksForPartition(replicaCountPerDatacenter, replicaCapacityGB);
      partitions.add(layout.addNewPartition(disks, replicaCapacityGB));
      numPartitions--;
    }

    return partitions;
  }
}
