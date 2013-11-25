package com.github.ambry.clustermap;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

  // TODO: Move to utils after re-based.
  private static void writeStringToFile(String string, String path) throws IOException {
    FileWriter fileWriter = null;
    try {
      File clusterFile = new File(path);
      fileWriter = new FileWriter(clusterFile);
      fileWriter.write(string);
    } finally {
      if (fileWriter != null) {
        fileWriter.close();
      }
    }
  }

  // TODO: Move to utils after re-based.
  private static void writeJsonToFile(JSONObject jsonObject, String path) throws IOException, JSONException {
    writeStringToFile(jsonObject.toString(2), path);
  }

  // TODO: Move to utils after re-based.
  private static String readStringFromFile(String path) throws IOException {
    // Code cribbed from StackOverflow and is similar to IO Commons and Guava
    // http://stackoverflow.com/questions/326390/how-to-create-a-java-string-from-the-contents-of-a-file
    byte[] encoded = Files.readAllBytes(Paths.get(path));
    return Charset.defaultCharset().decode(ByteBuffer.wrap(encoded)).toString();
  }

  // TODO: Move to utils after re-based.
  private static JSONObject readJsonFromFile(String path) throws IOException, JSONException {
    return new JSONObject(readStringFromFile(path));
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
  public PartitionId getPartitionIdFromBytes(byte[] bytes) {
    return partitionLayout.getPartition(bytes);
  }

  @Override
  public long getWritablePartitionIdsCount() {
    return getWritablePartitionIds().size();
  }

  @Override
  public PartitionId getWritablePartitionIdAt(long index) {
    // TODO: Ints.checkedCast(index) seems much better to me. Why not use Guava? Should we implement checkedCast in
    // utils if we don't import Guava?
    // return partitionLayout.getWritablePartitionIds().get(Ints.checkedCast(index));
    return partitionLayout.getWritablePartitions().get((int)index);
  }

  @Override
  public DataNodeId getDataNodeId(String hostname, int port) {
    return hardwareLayout.findDataNode(hostname, port);
  }

  @Override
  public List<? extends ReplicaId> getReplicaIds(DataNodeId dataNodeId) {
    return getReplicas(dataNodeId);
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

  public long getRawCapacityGB() {
    return hardwareLayout.getCapacityGB();
  }

  public long getAllocatedCapacityGB() {
    return partitionLayout.getCapacityGB();
  }

  public long getAllocatedCapacityGB(Datacenter datacenter) {
    long allocatedCapacityGB = 0;
    for (Partition partition : partitionLayout.getPartitions()) {
      for (Replica replica : partition.getReplicas()) {
        if (replica.getDisk().getDataNode().getDatacenter().equals(datacenter)) {
          allocatedCapacityGB += replica.getCapacityGB();
        }
      }
    }
    return allocatedCapacityGB;
  }

  public long getAllocatedCapacityGB(DataNodeId dataNode) {
    long allocatedCapacityGB = 0;
    for (Partition partition : partitionLayout.getPartitions()) {
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
    for (Partition partition : partitionLayout.getPartitions()) {
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

  public PartitionId addNewPartition(List<Disk> disks, long replicaCapacityGB) {
    return partitionLayout.addNewPartition(disks, replicaCapacityGB);
  }

  // Determine if there is enough capacity to allocate a PartitionId
  private boolean enoughFreeCapacity(int replicaCountPerDatacenter, long replicaCapacityGB) {
    for (Datacenter datacenter : hardwareLayout.getDatacenters()) {
      if (getFreeCapacityGB(datacenter) < replicaCountPerDatacenter * replicaCapacityGB) {
        logger.warn("Insufficient free space in datacenter {} ({} GB free)", datacenter.getName(), getFreeCapacityGB(datacenter));
        return false;
      }

      int rcpd = replicaCountPerDatacenter;
      for (DataNode dataNode : datacenter.getDataNodes()) {
        for (Disk disk : dataNode.getDisks()) {
          if (getFreeCapacityGB(disk) >= replicaCapacityGB) {
            rcpd--;
            break; // Only one replica per DataNodeId.
          }
        }
      }
      if (rcpd > 0) {
        logger.warn("Insufficient DataNodes ({}) with free space in datacenter {} for {} Replicas)", rcpd, datacenter.getName(), replicaCountPerDatacenter);
        return false;
      }
    }

    return true;
  }

  // Allocate unique datanode.disks for each replica in each datacenter up to replicaCountPerDatacenter
  private List<Disk> allocateDisksForPartition(int replicaCountPerDatacenter, long replicaCapacityGB) {
    ArrayList<Disk> allocatedDisks = new ArrayList<Disk>();

    for (Datacenter datacenter : hardwareLayout.getDatacenters()) {
      List<DataNode> shuffledDataNodes = new ArrayList<DataNode>(datacenter.getDataNodes());
      Collections.shuffle(shuffledDataNodes);

      int rcpd = replicaCountPerDatacenter;
      for (DataNode dataNode : shuffledDataNodes) {
        List<Disk> shuffledDisks = new ArrayList<Disk>(dataNode.getDisks());
        Collections.shuffle(shuffledDisks);

        for (Disk disk : shuffledDisks) {
          if (disk.getCapacityGB() > replicaCapacityGB) {
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
  // Hackish 1st attempt at PartitionId allocation policy to confirm Administrative API is sufficient.
  public List<PartitionId> allocatePartitions(int numPartitions, int replicaCountPerDatacenter, long replicaCapacityGB) {
    ArrayList<PartitionId> partitions = new ArrayList<PartitionId>(numPartitions);

    while (enoughFreeCapacity(replicaCountPerDatacenter, replicaCapacityGB) && numPartitions > 0) {
      List<Disk> disks = allocateDisksForPartition(replicaCountPerDatacenter, replicaCapacityGB);
      partitions.add(partitionLayout.addNewPartition(disks, replicaCapacityGB));
      numPartitions--;
    }

    return partitions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ClusterMapManager that = (ClusterMapManager) o;

    if (hardwareLayout != null ? !hardwareLayout.equals(that.hardwareLayout) : that.hardwareLayout != null)
      return false;
    if (partitionLayout != null ? !partitionLayout.equals(that.partitionLayout) : that.partitionLayout != null)
      return false;

    return true;
  }
}
