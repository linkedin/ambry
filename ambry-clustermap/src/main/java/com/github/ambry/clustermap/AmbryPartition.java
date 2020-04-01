/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.json.JSONArray;
import org.json.JSONObject;

import static com.github.ambry.clustermap.ClusterMapSnapshotConstants.*;


/**
 * {@link PartitionId} implementation to use within dynamic cluster managers.
 */
public class AmbryPartition implements PartitionId {
  private final long id;
  private final String partitionClass;
  private final ClusterManagerCallback<AmbryReplica, AmbryDisk, AmbryPartition, AmbryDataNode> clusterManagerCallback;
  private final Lock stateChangeLock = new ReentrantLock();

  private volatile PartitionState state;
  private long lastUpdatedSealedStateChangeCounter = 0;

  private static final short VERSION_FIELD_SIZE_IN_BYTES = 2;
  private static final short CURRENT_VERSION = 1;
  private static final int PARTITION_SIZE_IN_BYTES = VERSION_FIELD_SIZE_IN_BYTES + 8;

  /**
   * Instantiate an AmbryPartition instance.
   * @param id the id associated with this partition.
   * @param partitionClass the partition class that this partition belongs to
   * @param clusterManagerCallback the {@link HelixClusterManager.HelixClusterManagerCallback} to use to make callbacks
   *                               to the {@link HelixClusterManager}
   * The initial state defaults to {@link PartitionState#READ_WRITE}.
   */
  AmbryPartition(long id, String partitionClass,
      ClusterManagerCallback<AmbryReplica, AmbryDisk, AmbryPartition, AmbryDataNode> clusterManagerCallback) {
    this.id = id;
    this.partitionClass = partitionClass;
    this.clusterManagerCallback = clusterManagerCallback;
    this.state = PartitionState.READ_WRITE;
  }

  @Override
  public byte[] getBytes() {
    return ClusterMapUtils.serializeShortAndLong(CURRENT_VERSION, id);
  }

  @Override
  public List<AmbryReplica> getReplicaIds() {
    return clusterManagerCallback.getReplicaIdsForPartition(this);
  }

  @Override
  public List<AmbryReplica> getReplicaIdsByState(ReplicaState state, String dcName) {
    return clusterManagerCallback.getReplicaIdsByState(this, state, dcName);
  }

  @Override
  public PartitionState getPartitionState() {
    resolvePartitionState();
    return state;
  }

  @Override
  public boolean isEqual(String other) {
    return Long.toString(id).equals(other);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    AmbryPartition partition = (AmbryPartition) o;
    return id == partition.id;
  }

  @Override
  public int hashCode() {
    return Long.hashCode(id);
  }

  @Override
  public String toString() {
    return "Partition[" + id + "]";
  }

  @Override
  public int compareTo(PartitionId o) {
    AmbryPartition other = (AmbryPartition) o;
    return Long.compare(id, other.id);
  }

  /**
   * Return the byte representation of the partition id from the given stream.
   * @param stream the {@link DataInputStream} containing the id
   * @return the byte representation fo the partition id.
   * @throws IOException if the read from the stream encounters an IOException.
   */
  static byte[] readPartitionBytesFromStream(InputStream stream) throws IOException {
    return Utils.readBytesFromStream(stream, PARTITION_SIZE_IN_BYTES);
  }

  /**
   * Construct name based on the id that is appropriate for use as a file or directory name.
   * @return string representation of the id for use as part of file system path.
   */
  @Override
  public String toPathString() {
    return Long.toString(id);
  }

  @Override
  public String getPartitionClass() {
    return partitionClass;
  }

  @Override
  public JSONObject getSnapshot() {
    JSONObject snapshot = new JSONObject();
    snapshot.put(PARTITION_ID, toPathString());
    snapshot.put(PARTITION_WRITE_STATE, getPartitionState().name());
    snapshot.put(PARTITION_CLASS, getPartitionClass());
    JSONArray replicas = new JSONArray();
    getReplicaIds().forEach(replica -> replicas.put(replica.getSnapshot()));
    snapshot.put(PARTITION_REPLICAS, replicas);
    return snapshot;
  }

  /**
   * Take actions, if any, on being notified that this partition has become {@link PartitionState#READ_ONLY}
   */
  void onPartitionReadOnly() {
    // no-op. The static manager does not deal with this. In the dynamic world, the cluster manager will rely
    // entirely on Helix for this.
  }

  /**
   * Resolves the {@link PartitionState} based on the state of the replicas.
   */
  void resolvePartitionState() {
    // If there was a change to the sealed state of replicas in the cluster manager since the last check, refresh the
    // state of this partition. We do this to avoid querying every time this method is called, considering how
    // update to sealed states of replicas are relatively rare.
    long currentCounterValue = clusterManagerCallback.getSealedStateChangeCounter();
    // if the lock could not be taken, that means the state is being updated in another thread. Avoid updating the
    // state in that case.
    if (currentCounterValue > lastUpdatedSealedStateChangeCounter && stateChangeLock.tryLock()) {
      try {
        lastUpdatedSealedStateChangeCounter = currentCounterValue;
        boolean isSealed = false;
        for (AmbryReplica replica : clusterManagerCallback.getReplicaIdsForPartition(this)) {
          if (replica.isSealed()) {
            isSealed = true;
            break;
          }
        }
        state = isSealed ? PartitionState.READ_ONLY : PartitionState.READ_WRITE;
      } finally {
        stateChangeLock.unlock();
      }
    }
  }
}
