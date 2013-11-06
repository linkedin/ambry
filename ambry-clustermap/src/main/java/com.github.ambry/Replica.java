package com.github.ambry;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;

/**
 * Replica represents one replica of a partition in Ambry. Each Replica is uniquely identifiable by a ReplicaId
 * (effectively a PartitionId-DiskId pair) Note that this induces a constraint that a partition can never have more than
 * one replica on a given disk. This ensures replicas do not share a lowest-level single-point-of-failure.
 * <p/>
 * The state of a replica is a function of its partition state and disk state.
 */
public class Replica {
  ReplicaId replicaId;

  Partition partition;
  Disk disk;


  public Replica(Partition partition, Disk disk) {
    this.replicaId = new ReplicaId(partition.getPartitionId(), disk.getDiskId());

    this.partition = partition;
    this.disk = disk;

    validate();
  }

  public Replica(Partition partition, JSONObject jsonObject) throws JSONException {
    this.replicaId = new ReplicaId(new JSONObject(jsonObject.getString("replicaId")));

    this.partition = partition;
    this.disk = partition.getLayout().getCluster().getDisk(replicaId.getDiskId());

    validate();
  }

  public Partition getPartition() {
    return partition;
  }

  public Disk getDisk() {
    return disk;
  }


  public ReplicaId getReplicaId() {
    return replicaId;
  }

  public long getCapacityGB() {
    return partition.getReplicaCapacityGB();
  }

  protected void validatePartitionId() {
    // Do not call partition.validate(). Could introduce infinite call cycle.
    if (!replicaId.getPartitionId().equals(partition.getPartitionId())) {
      throw new IllegalStateException("Invalid Replica. PartitionId in ReplicaId does not match Partition's id:" +
              replicaId.getPartitionId() + " != " + partition.getPartitionId());
    }
  }

  public void validate() {
    replicaId.validate();
    validatePartitionId();
    // Do not call disk.validate(). Could introduce infinite call cycle.
    // TODO: Add validation that only one replica per DataNode?
    // No need to validate disk id since it is looked up from cluster.
  }

  // Returns JSON representation
  // Serialize PartitionId and DiskId not Partition and Disk.
  @Override
  public String toString() {
    try {
      return new JSONStringer()
              .object()
              .key("replicaId")
              .value(replicaId)
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

    Replica replica = (Replica) o;

    if (!replicaId.equals(replica.replicaId)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return replicaId.hashCode();
  }
}
