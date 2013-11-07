package com.github.ambry;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Partition is the unit of data management in Ambry. Blobs are stored in partitions. Partitions consist of replicas and
 * so are fault tolerant. Each Partition is uniquely identifiable by its PartitionId.
 */
public class Partition {
  public enum State {
    READ_WRITE,
    READ_ONLY
  }

  Layout layout;

  PartitionId partitionId;
  State state;
  // TODO: add replication policy object.
  long replicaCapacityGB;
  ArrayList<Replica> replicas;

  private Logger logger = LoggerFactory.getLogger(getClass());


  public Partition(Layout layout, PartitionId partitionId, long replicaCapacityGB) {
    this.layout = layout;

    this.partitionId = partitionId;
    this.state = State.READ_WRITE;
    this.replicaCapacityGB = replicaCapacityGB;
    this.replicas = new ArrayList<Replica>();

    validate();
  }

  public Partition(Layout layout, JSONObject jsonObject) throws JSONException {
    this.layout = layout;

    this.partitionId = new PartitionId(new JSONObject(jsonObject.getString("partitionId")));
    this.state = State.valueOf(jsonObject.getString("state"));
    this.replicaCapacityGB = jsonObject.getLong("replicaCapacityGB");
    this.replicas = new ArrayList<Replica>();
    for (int i = 0; i < jsonObject.getJSONArray("replicas").length(); ++i) {
      this.replicas.add(new Replica(this, jsonObject.getJSONArray("replicas").getJSONObject(i)));
    }

    validate();
  }

  public Layout getLayout() {
    return layout;
  }

  public PartitionId getPartitionId() {
    return partitionId;
  }

  public State getState() {
    return state;
  }

  public long getCapacityGB() {
    return replicaCapacityGB * replicas.size();
  }

  public long getReplicaCapacityGB() {
    return replicaCapacityGB;
  }

  public List<Replica> getReplicas() {
    return Collections.unmodifiableList(replicas);
  }

  public void addReplica(Replica replica) {
    replicas.add(replica);
  }

  protected void validateLayout() {
    if (layout == null) {
      throw new IllegalStateException("Layout of a Partition cannot be null");
    }
  }

  protected boolean isStateValid() {
    for (State validState : State.values()) {
      if (state == validState) {
        return true;
      }
    }
    return false;
  }

  protected void validateState() {
    if (!isStateValid()) {
      throw new IllegalStateException("Invalid partition state: " + state);
    }
  }

  protected void validateCapacity() {
    if (replicaCapacityGB <= 0) {
      throw new IllegalStateException("Invalid disk capacity: " + replicaCapacityGB);
    }
  }


  public void validate() {
    validateLayout();

    partitionId.validate();
    validateState();
    validateCapacity();
    for (Replica replica : replicas) {
      replica.validate();
    }
  }

  public JSONObject toJSONObject() throws JSONException {
    JSONObject jsonObject = new JSONObject()
            .put("partitionId", partitionId.toJSONObject())
            .put("state", state)
            .put("replicaCapacityGB", replicaCapacityGB)
            .put("replicas", new JSONArray());
    for (Replica replica : replicas) {
      jsonObject.accumulate("replicas", replica.toJSONObject());
    }
    return jsonObject;
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

    Partition partition = (Partition) o;

    if (replicaCapacityGB != partition.replicaCapacityGB) return false;
    if (!partitionId.equals(partition.partitionId)) return false;
    if (!replicas.equals(partition.replicas)) return false;
    if (state != partition.state) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = partitionId.hashCode();
    result = 31 * result + state.hashCode();
    result = 31 * result + replicas.hashCode();
    result = 31 * result + (int) (replicaCapacityGB ^ (replicaCapacityGB >>> 32));
    return result;
  }
}
