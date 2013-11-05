package com.github.ambry;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Partition is the unit of data management in Ambry. Blobs are stored in partitions. Partitions consist of replicas and
 * so are fault tolerant. Each Partition is uniquely identifiable by its PartitionId.
 */
public class Partition {
  Layout layout;

  PartitionId partitionId;
  // TODO: policy object to represent number of replicas and constraints on replicas
  ArrayList<Replica> replicas;

  public Partition(Layout layout, PartitionId partitionId) {
    this.layout = layout;

    this.partitionId = partitionId;
    this.replicas = new ArrayList<Replica>();

    validate();
  }

  public Partition(Layout layout, JSONObject jsonObject)  throws JSONException {
    this.layout = layout;

    this.partitionId = new PartitionId(new JSONObject(jsonObject.getString("partitionId")));
    this.replicas = new ArrayList<Replica>();
    JSONArray diskJSONArray = jsonObject.getJSONArray("replicas");
    for (int i = 0; i < diskJSONArray.length(); ++i) {
      this.replicas.add(new Replica(this, new JSONObject(diskJSONArray.getString(i))));
    }

    validate();
  }

  public Layout getLayout() {
    return layout;
  }

  public PartitionId getPartitionId() {
    return partitionId;
  }

  public List<Replica> getReplicas() {
    return Collections.unmodifiableList(replicas);
  }

  public void addReplica(Replica replica) {
    replicas.add(replica);
  }

  protected void validateLayout() {
    if(layout == null) {
      throw new IllegalStateException("Layout of a Partition cannot be null");
    }
  }

  public void validate() {
    validateLayout();

    partitionId.validate();
    // TODO: validate replication policy? I.e., are there enough replicas?
    for(Replica replica  : replicas) {
      replica.validate();
    }
  }


  // Returns JSON representation
  @Override
  public String toString() {
    try {
      return new JSONStringer()
              .object()
              .key("partitionId")
              .value(partitionId)
              .key("replicas")
              .value(replicas)
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

    Partition partition = (Partition) o;

    if (!partitionId.equals(partition.partitionId)) return false;
    if (!replicas.equals(partition.replicas)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = partitionId.hashCode();
    result = 31 * result + replicas.hashCode();
    return result;
  }
}
