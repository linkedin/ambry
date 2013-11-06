package com.github.ambry;

/**
 * TODOs for ambry-clustermap
 * - Names of all base types in this package need to be reviewed carefully now. Changing names later is a pain,
 *   and names matter!
 * - Should we avoid using bare base types for key types. For example, String for DataNode hostname and for Datacenter
 *   name If we use bare base types, some interfaces are not really typesafe.
 * - Add logging
 * - Need more unit tests
 * - Add interface to api & javadoc
 */

// TODO:
// - DiskId is Node.hostname:/mnt
// - File SerDe
// - ClusterMapAPI
// - convert hostname to fully qualfied hostname

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * ClusterMap allows components in Ambry to query the topology. This covers both cluster hardware and data layout.
 */
// TODO: Rename this ClusterMapManager
public class ClusterMap {
  protected Cluster cluster;
  protected Layout layout;


  public ClusterMap(Cluster cluster, Layout layout) {
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

  public long getFreeCapacityGB() {
    return getRawCapacityGB() - getAllocatedCapacityGB();
  }

  public Datacenter addNewDataCenter(String datacenterName) {
    return cluster.addNewDataCenter(datacenterName);
  }

  public DataNode addNewDataNode(String datacenterName, String hostname) {
    return cluster.addNewDataNode(datacenterName, hostname);
  }

  public Disk addNewDisk(String hostname, long capacityGB) {
    return cluster.addNewDisk(hostname, capacityGB);
  }

  public Partition addNewPartition(List<Disk> disks, long replicaCapacityGB) {
    return layout.addNewPartition(disks, replicaCapacityGB);
  }

  // TODO: Is this API needed (at this time) (at this level)?
  public Replica addNewReplicaToPartition(Partition partition, Disk disk) {
    return layout.addNewReplicaToPartition(partition, disk);
  }


  // Determine if there is enough capacity to allocate a Partition
  private boolean enoughRawCapacity(int replicaCountPerDatacenter, long replicaCapacityGB) {
    for(Datacenter datacenter : cluster.getDatacenters()) {
      if (datacenter.getCapacityGB() < replicaCountPerDatacenter * replicaCapacityGB) {
        // Log
        return false;
      }

      int rcpd = replicaCountPerDatacenter;
      for (DataNode dataNode : datacenter.getDataNodes()) {
        for (Disk disk : dataNode.getDisks()) {
          if (disk.getCapacityGB() > replicaCapacityGB) {
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
  // Hackish 1st attempt at allocation policy to confirm Administrative API is sufficient
  public List<Partition> allocatePartitions(int numPartitions, long replicaCapacityGB) {
    ArrayList<Partition> partitions = new ArrayList<Partition>();
    final int replicaCountPerDatacenter = 2;


    while (enoughRawCapacity(replicaCountPerDatacenter, replicaCapacityGB) && numPartitions > 0) {
      List<Disk> disks = allocateDisksForPartition(replicaCountPerDatacenter, replicaCapacityGB);
      layout.addNewPartition(disks, replicaCapacityGB);
      numPartitions--;
    }

    return partitions;
  }
}
