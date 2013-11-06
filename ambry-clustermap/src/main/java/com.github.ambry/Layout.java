package com.github.ambry;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;

import java.util.*;

/**
 * Layout of partitions and replicas on a cluster.
 */
public class Layout {
  Cluster cluster;
  // TODO: Add version number and/or timestamp and/or username of last writer for cluster

  private PartitionId prevPartitionId;
  private ArrayList<Partition> partitions;

  private Map<PartitionId, Partition> partitionMap;
  private Map<ReplicaId, Replica> replicaMap;

  public Layout(Cluster cluster, JSONObject jsonObject) throws JSONException {
    this.cluster = cluster;

    String clusterName = jsonObject.getString("clusterName");
    if (!cluster.getName().equals(clusterName)) {
      throw new IllegalStateException("Cluster name in json does not match name of cluster:"
              + clusterName + " != " + cluster.getName());
    }

    this.prevPartitionId = null;
    if (!jsonObject.isNull("prevPartitionId")) {
      this.prevPartitionId = new PartitionId(new JSONObject(jsonObject.getString("prevPartitionId")));
    }

    this.partitions = new ArrayList<Partition>();
    JSONArray partitionJSONArray = jsonObject.getJSONArray("partitions");
    for (int i = 0; i < partitionJSONArray.length(); ++i) {
      this.partitions.add(new Partition(this, new JSONObject(partitionJSONArray.getString(i))));
    }

    buildMaps();
    validate();
  }

  public Layout(Cluster cluster) {
    this.cluster = cluster;

    this.prevPartitionId = null;
    this.partitions = new ArrayList<Partition>();

    buildMaps();
    validate();
  }

  private void buildMaps() {
    this.partitionMap = new HashMap<PartitionId, Partition>();
    this.replicaMap = new HashMap<ReplicaId, Replica>();

    for (Partition partition : partitions) {
      if (partitionMap.put(partition.getPartitionId(), partition) != null) {
        throw new IllegalStateException("PartitionId must be unique: " + partition.getPartitionId());
      }

      for(Replica replica : partition.getReplicas()) {
        if(replicaMap.put(replica.getReplicaId(), replica) != null) {
          throw new IllegalStateException("ReplicaId must be unique: " + replica.getReplicaId());
        }
      }
    }
  }

  public Cluster getCluster() {
    return cluster;
  }

  public List<Partition> getPartitions() {
    return Collections.unmodifiableList(partitions);
  }

  public Partition getPartition(PartitionId partitionId) {
    return partitionMap.get(partitionId);
  }

  public Replica getReplica(ReplicaId replicaId) {
    return replicaMap.get(replicaId);
  }

  public long getCapacityGB() {
    long capacityGB = 0;
    for(Partition partition : partitions) {
      capacityGB += partition.getReplicaCapacityGB();
    }
    return capacityGB;
  }


  public void validate() {
    if (prevPartitionId != null) {
      this.prevPartitionId.validate();
    }
    for (Partition partition : partitions) {
      partition.validate();
    }
  }

  protected PartitionId getNewPartitionId() {
    if(prevPartitionId == null) {
      prevPartitionId = PartitionId.getFirstPartitionId();
      return prevPartitionId ;
    } else {
      prevPartitionId  = PartitionId.getNewPartitionId(prevPartitionId);
      return prevPartitionId ;
    }
  }

  public void addPartition(Partition partition) {
    partitions.add(partition);
  }

  // Creates a Partition and corresponding Replicas for each specified disk id
  public Partition addNewPartition(List<Disk> disks, long replicaCapacityGB) {
    PartitionId partitionId = getNewPartitionId();

    if (partitionMap.containsKey(partitionId)) {
      throw new IllegalArgumentException("Partition Id already in use. Must be unique. " + partitionId);
    }
    Partition partition = new Partition(this, partitionId, replicaCapacityGB);
    addPartition(partition);
    partitionMap.put(partitionId, partition);

    if(disks != null) {
      for(Disk disk : disks) {
        addNewReplicaToPartition(partition, disk);
      }
    }

    return partition;
  }

  public Replica addNewReplicaToPartition(Partition partition, Disk disk) {
    if (cluster.getDisk(disk.getDiskId()) == null) {
      throw new IllegalArgumentException("Specified Disk is not part of cluster: " + disk);
    }

    Replica replica = new Replica(partition, disk);
    if(replicaMap.put(replica.getReplicaId(), replica) != null) {
      throw new IllegalArgumentException("Replica Id already in use. Must be unique: " + replica.getReplicaId());
    }
    partition.addReplica(replica);

    return replica;
  }

  // Returns JSON representation
  @Override
  public String toString() {
    try {
      return new JSONStringer()
              .object()
              .key("clusterName")
              .value(cluster.getName())
              .key("prevPartitionId")
              .value(prevPartitionId)
              .key("partitions")
              .value(partitions)
              .endObject()
              .toString();
    } catch (JSONException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    return null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Layout layout = (Layout) o;

    if (!partitions.equals(layout.partitions)) return false;
    if (prevPartitionId != null ? !prevPartitionId.equals(layout.prevPartitionId) : layout.prevPartitionId != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = prevPartitionId != null ? prevPartitionId.hashCode() : 0;
    result = 31 * result + partitions.hashCode();
    return result;
  }
}
