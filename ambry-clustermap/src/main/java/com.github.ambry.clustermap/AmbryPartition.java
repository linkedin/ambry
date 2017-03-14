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
import java.nio.ByteBuffer;
import java.util.List;


/**
 * {@link PartitionId} implementation to use within dynamic cluster managers.
 */
class AmbryPartition extends PartitionId {
  private final Long id;
  private final HelixClusterManager.HelixClusterManagerCallback clusterManagerCallback;
  private volatile PartitionState state;

  private static final short Version_Field_Size_In_Bytes = 2;
  private static final short Current_Version = 1;
  private static final int Partition_Size_In_Bytes = Version_Field_Size_In_Bytes + 8;

  /**
   * Instantiate an AmbryParitition instance.
   * @param id the id associated with this partition.
   * @param clusterManagerCallback the {@link HelixClusterManager.HelixClusterManagerCallback} to use to make callbacks
   *                               to the {@link HelixClusterManager}
   * The initial state defaults to {@link PartitionState#READ_WRITE}.
   */
  AmbryPartition(long id, HelixClusterManager.HelixClusterManagerCallback clusterManagerCallback) {
    this.id = id;
    this.clusterManagerCallback = clusterManagerCallback;
    this.state = PartitionState.READ_WRITE;
  }

  @Override
  public byte[] getBytes() {
    ByteBuffer buffer = ByteBuffer.allocate(Partition_Size_In_Bytes);
    buffer.putShort(Current_Version);
    buffer.putLong(id);
    return buffer.array();
  }

  @Override
  public List<? extends ReplicaId> getReplicaIds() {
    return clusterManagerCallback.getReplicaIdsForPartition(this);
  }

  @Override
  public PartitionState getPartitionState() {
    return state;
  }

  @Override
  public boolean isEqual(String other) {
    return id.toString().equals(other);
  }

  @Override
  public String toString() {
    return "Partition[" + id + "]";
  }

  @Override
  public int compareTo(PartitionId o) {
    AmbryPartition other = (AmbryPartition) o;
    return id.compareTo(other.id);
  }

  /**
   * Return the byte representation of the partition id from the given stream.
   * @param stream the {@link DataInputStream} containing the id
   * @return the byte representation fo the partition id.
   * @throws IOException if the read from the stream encounters an IOException.
   */
  static byte[] readPartitionBytesFromStream(DataInputStream stream) throws IOException {
    return Utils.readBytesFromStream(stream, Partition_Size_In_Bytes);
  }

  /**
   * Construct name based on the id that is appropriate for use as a file or directory name.
   * @return string representation of the id for use as part of file system path.
   */
  String toPathString() {
    return id.toString();
  }

  /**
   * Set the state of this partition.
   * @param newState the updated {@link PartitionState}.
   */
  void setState(PartitionState newState) {
    state = newState;
  }

  /**
   * Take actions, if any, on being notified that this partition has become {@link PartitionState#READ_ONLY}
   */
  void onPartitionReadOnly() {
    // no-op. The static manager does not deal with this. In the dynamic world, the cluster manager will rely
    // entirely on Helix for this.
  }
}

