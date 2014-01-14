package com.github.ambry.clustermap;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A Partition is the unit of data management in Ambry. Each Partition is uniquely identifiable by an ID. Partitions
 * consist of one or more {@link Replica}s. Replicas ensure that a Partition is available and reliable.
 */
public class Partition extends PartitionId {

  private static final long MinReplicaCapacityInBytes = 1 * 1024 * 1024 * 1024L;
  private static final long MaxReplicaCapacityInBytes = 10995116277760L; // 10 TB
  private static final int Partition_Size_In_Bytes = 8;

  private Long id;
  PartitionState partitionState;
  long replicaCapacityInBytes;
  List<Replica> replicas;

  private Logger logger = LoggerFactory.getLogger(getClass());

  // For constructing new Partition
  public Partition(long id, PartitionState partitionState, long replicaCapacityInBytes) {
    this.id = id;
    this.partitionState = partitionState;
    this.replicaCapacityInBytes = replicaCapacityInBytes;
    this.replicas = new ArrayList<Replica>();

    validate();
  }

  public Partition(PartitionLayout partitionLayout, JSONObject jsonObject) throws JSONException {
    this(partitionLayout.getHardwareLayout(), jsonObject);
  }

  public Partition(HardwareLayout hardwareLayout, JSONObject jsonObject) throws JSONException {
    this.id = jsonObject.getLong("id");
    this.partitionState = PartitionState.valueOf(jsonObject.getString("partitionState"));
    this.replicaCapacityInBytes = jsonObject.getLong("replicaCapacityInBytes");
    this.replicas = new ArrayList<Replica>(jsonObject.getJSONArray("replicas").length());
    for (int i = 0; i < jsonObject.getJSONArray("replicas").length(); ++i) {
      this.replicas.add(i, new Replica(hardwareLayout, this, jsonObject.getJSONArray("replicas").getJSONObject(i)));
    }

    validate();
  }

  public static byte[] readPartitionBytesFromStream(DataInputStream stream) throws IOException {
    byte[] partitionBytes = new byte[Partition_Size_In_Bytes];
    stream.read(partitionBytes);
    return partitionBytes;
  }

  @Override
  public byte[] getBytes() {
    ByteBuffer buffer = ByteBuffer.allocate(Partition_Size_In_Bytes);
    buffer.putLong(id);
    return buffer.array();
  }

  @Override
  public List<ReplicaId> getReplicaIds() {
    List<Replica> replicas = getReplicas();
    return new ArrayList<ReplicaId>(replicas);
  }

  @Override
  public PartitionState getPartitionState() {
    return partitionState;
  }

  public long getCapacityInBytes() {
    return replicaCapacityInBytes * replicas.size();
  }

  public long getReplicaCapacityInBytes() {
    return replicaCapacityInBytes;
  }

  public List<Replica> getReplicas() {
    return replicas;
  }

  /**
   * Construct name based on Partition ID appropriate for use as a file or directory name.
   *
   * @return string representation of the Partition's ID for use as part of file system path.
   */
  public String toPathString() {
    return Long.toString(id);
  }

  // For constructing new Partition
  public void addReplica(Replica replica) {
    replicas.add(replica);

    validate();
  }

  protected void validateReplicaCapacityInBytes() {
    if (replicaCapacityInBytes < MinReplicaCapacityInBytes) {
      throw new IllegalStateException("Invalid disk capacity: " + replicaCapacityInBytes
                                      + " is less than " + MinReplicaCapacityInBytes);
    }
    else if (replicaCapacityInBytes > MaxReplicaCapacityInBytes) {
      throw new IllegalStateException("Invalid disk capacity: " + replicaCapacityInBytes
                                      + " is more than " + MaxReplicaCapacityInBytes);
    }
  }

  protected void validateConstraints() {
    // Ensure each replica is on distinct Disk and DataNode.
    Set<DataNode> dataNodeSet = new HashSet<DataNode>();
    Set<Disk> diskSet = new HashSet<Disk>();

    for (Replica replica : replicas) {
      if (!diskSet.add((Disk)replica.getDiskId())) {
        throw new IllegalStateException("Multiple Replicas for same Partition are layed out on same Disk: "
                                        + toString());
      }
      if (!dataNodeSet.add(((Disk)replica.getDiskId()).getDataNode())) {
        throw new IllegalStateException("Multiple Replicas for same Partition are layed out on same DataNode: "
                                        + toString());
      }
    }
  }

  protected void validate() {
    logger.trace("begin validate.");
    validateReplicaCapacityInBytes();
    validateConstraints();
    logger.trace("complete validate.");
  }

  public JSONObject toJSONObject() throws JSONException {
    JSONObject jsonObject = new JSONObject()
            .put("id", id)
            .put("partitionState", partitionState)
            .put("replicaCapacityInBytes", replicaCapacityInBytes)
            .put("replicas", new JSONArray());
    for (Replica replica : replicas) {
      jsonObject.accumulate("replicas", replica.toJSONObject());
    }
    return jsonObject;
  }

  @Override
  public String toString() {
    return "Partition: " + toPathString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Partition partition = (Partition)o;

    if (id != partition.id) return false;

    return true;
  }

  @Override
  public int hashCode() {
    return (int)(id ^ (id >>> 32));
  }

  @Override
  public int compareTo(PartitionId o) {
    if (o == null)
      throw new NullPointerException("input argument null");

    Partition other = (Partition)o;
    return id.compareTo(other.id);
  }
}
