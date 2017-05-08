/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.clustermap;

import com.github.ambry.utils.Utils;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.clustermap.ClusterMapUtils.*;


/**
 * An extension of {@link PartitionId} to be used within the {@link StaticClusterManager}.
 *
 * A Partition is the unit of data management in Ambry. Each Partition is uniquely identifiable by an ID. Partitions
 * consist of one or more {@link Replica}s. Replicas ensure that a Partition is available and reliable.
 */
class Partition extends PartitionId {

  private static final short Version_Field_Size_In_Bytes = 2;
  private static final short Current_Version = 1;
  private static final int Partition_Size_In_Bytes = Version_Field_Size_In_Bytes + 8;

  private Long id;
  PartitionState partitionState;
  long replicaCapacityInBytes;
  List<Replica> replicas;

  private Logger logger = LoggerFactory.getLogger(getClass());

  // For constructing new Partition
  Partition(long id, PartitionState partitionState, long replicaCapacityInBytes) {
    logger.trace("Partition " + id + ", " + partitionState + ", " + replicaCapacityInBytes);
    this.id = id;
    this.partitionState = partitionState;
    this.replicaCapacityInBytes = replicaCapacityInBytes;
    this.replicas = new ArrayList<Replica>();

    validate();
  }

  Partition(PartitionLayout partitionLayout, JSONObject jsonObject) throws JSONException {
    this(partitionLayout.getHardwareLayout(), jsonObject);
  }

  Partition(HardwareLayout hardwareLayout, JSONObject jsonObject) throws JSONException {
    if (logger.isTraceEnabled()) {
      logger.trace("Partition " + jsonObject.toString());
    }
    this.id = jsonObject.getLong("id");
    this.partitionState = PartitionState.valueOf(jsonObject.getString("partitionState"));
    this.replicaCapacityInBytes = jsonObject.getLong("replicaCapacityInBytes");
    this.replicas = new ArrayList<Replica>(jsonObject.getJSONArray("replicas").length());
    for (int i = 0; i < jsonObject.getJSONArray("replicas").length(); ++i) {
      this.replicas.add(i, new Replica(hardwareLayout, this, jsonObject.getJSONArray("replicas").getJSONObject(i)));
    }

    validate();
  }

  static byte[] readPartitionBytesFromStream(InputStream stream) throws IOException {
    byte[] partitionBytes = Utils.readBytesFromStream(stream, Partition_Size_In_Bytes);
    return partitionBytes;
  }

  @Override
  public byte[] getBytes() {
    return ClusterMapUtils.serializeShortAndLong(Current_Version, id);
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

  @Override
  public boolean isEqual(String partitionId) {
    return id.toString().equals(partitionId);
  }

  long getAllocatedRawCapacityInBytes() {
    return replicaCapacityInBytes * replicas.size();
  }

  long getReplicaCapacityInBytes() {
    return replicaCapacityInBytes;
  }

  List<Replica> getReplicas() {
    return replicas;
  }

  long getId() {
    return id;
  }

  /**
   * Construct name based on Partition ID appropriate for use as a file or directory name.
   *
   * @return string representation of the Partition's ID for use as part of file system path.
   */
  String toPathString() {
    return Long.toString(id);
  }

  // For constructing new Partition
  void addReplica(Replica replica) {
    replicas.add(replica);

    validate();
  }

  private void validateConstraints() {
    // Ensure each replica is on distinct Disk and DataNode.
    Set<DataNode> dataNodeSet = new HashSet<DataNode>();
    Set<Disk> diskSet = new HashSet<Disk>();

    for (Replica replica : replicas) {
      if (!diskSet.add((Disk) replica.getDiskId())) {
        throw new IllegalStateException(
            "Multiple Replicas for same Partition are layed out on same Disk: " + toString());
      }
      if (!dataNodeSet.add(((Disk) replica.getDiskId()).getDataNode())) {
        throw new IllegalStateException(
            "Multiple Replicas for same Partition are layed out on same DataNode: " + toString());
      }
    }
  }

  private void validate() {
    logger.trace("begin validate.");
    validateReplicaCapacityInBytes(replicaCapacityInBytes);
    validateConstraints();
    logger.trace("complete validate.");
  }

  JSONObject toJSONObject() throws JSONException {
    JSONObject jsonObject = new JSONObject().put("id", id)
        .put("partitionState", partitionState)
        .put("replicaCapacityInBytes", replicaCapacityInBytes)
        .put("replicas", new JSONArray());
    for (Replica replica : replicas) {
      jsonObject.accumulate("replicas", replica.toJSONObject());
    }
    return jsonObject;
  }

  /**
   * Generates a {@link String} representation that uniquely identifies this partition. The string
   * is in the format of {@code Partition[i]}, where {@code i} is a {@code long} id number uniquely
   * associated with this partition.
   * @return The {@link String} representation of this partition.
   */
  @Override
  public String toString() {
    return "Partition[" + toPathString() + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Partition partition = (Partition) o;
    return id.equals(partition.id);
  }

  @Override
  public int hashCode() {
    return (int) (id ^ (id >>> 32));
  }

  @Override
  public int compareTo(PartitionId o) {
    if (o == null) {
      throw new NullPointerException("input argument null");
    }

    Partition other = (Partition) o;
    return id.compareTo(other.id);
  }

  void onPartitionReadOnly() {
    // no-op
  }
}
