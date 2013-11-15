package com.github.ambry.clustermap;

import com.github.ambry.clustermap.ReplicaContext;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Replica represents one replica of a partition in Ambry. Each Replica is uniquely identifiable by a ReplicaId
 * (effectively a PartitionId-DiskId pair) Note that this induces a constraint that a partition can never have more than
 * one replica on a given disk. This ensures replicas do not share a lowest-level single-point-of-failure.
 * <p/>
 * The state of a replica is a function of its partition state and disk state.
 */
public class Replica {
  private ReplicaId replicaId;

  private Partition partition;

  private Logger logger = LoggerFactory.getLogger(getClass());


  public Replica(Partition partition, DiskId diskId) {
    this.replicaId = new ReplicaId(partition.getPartitionId(), diskId);

    this.partition = partition;

    validate();
  }

  public Replica(Partition partition, Disk disk) {
    this(partition, disk.getDiskId());
  }

  public Replica(Partition partition, JSONObject jsonObject) throws JSONException {
    this.replicaId = new ReplicaId(jsonObject.getJSONObject("replicaId"));

    this.partition = partition;

    validate();
  }

  public Partition getPartition() {
    return partition;
  }

  public Disk getDisk() {
    return partition.getLayout().getCluster().getDisk(replicaId.getDiskId());
  }

  public ReplicaId getReplicaId() {
    return replicaId;
  }

  public long getCapacityGB() {
    return partition.getReplicaCapacityGB();
  }

  public ReplicaContext getReplicaContext() {
    return new ReplicaContext(replicaId.getDiskId().getDataNodeId().getHostname(),
            replicaId.getDiskId().getDataNodeId().getPort(),
            replicaId.getDiskId().getMountPath());
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
    // TODO: Add validation that only one replica per DataNode? Per Disk? Should probably do this at Layout validation.
    // No need to validate disk id since it is looked up from cluster.
  }

  public JSONObject toJSONObject() throws JSONException {
    return new JSONObject()
            .put("replicaId", replicaId.toJSONObject());
  }

  @Override
  public String toString() {
    try {
      return toJSONObject().toString();
    } catch (JSONException e) {
      logger.warn("JSONException caught in toString:" + e.getCause());
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
