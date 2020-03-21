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

import java.util.List;


/**
 * A PartitionId consists of one or more {@link ReplicaId}s. A PartitionId is uniquely identified by an ID.
 */
public interface PartitionId extends Resource, Comparable<PartitionId> {
  /**
   * Serializes the ID of this PartitionId to bytes.
   *
   * @return byte-serialized ID of this PartitionId.
   */
  byte[] getBytes();

  /**
   * Gets Replicas that comprise this PartitionId.
   *
   * @return list of the Replicas that comprise this PartitionId.
   */
  List<? extends ReplicaId> getReplicaIds();

  /**
   * Gets Replicas from specified datacenter that are in required state.
   * @param state the {@link ReplicaState}
   * @param dcName the name of datacenter from which the replica should come. If null, choose replicas from all datacenters.
   * @return list of Replicas that satisfy requirement.
   */
  List<? extends ReplicaId> getReplicaIdsByState(ReplicaState state, String dcName);

  /**
   * Gets the state of this PartitionId.
   *
   * @return state of this PartitionId.
   */
  PartitionState getPartitionState();

  /**
   * Compares the PartitionId to a string representation of another PartitionId
   * @param partitionId  The string form of the partition that needs to be compared against
   * @return True, if the partitions match, false otherwise
   */
  boolean isEqual(String partitionId);

  /**
   * Returns a {@link String} that uniquely represents the {@code PartitionId}.
   * @return String representation of the {@code PartitionId}.
   */
  @Override
  String toString();

  /**
   * Returns a strictly numerical {@link String} that uniquely represents the {@code PartitionId}.
   * @return Strictly numerical string representation of the {@code PartitionId}.
   */
  String toPathString();

  /**
   * @return the partition class that this partition belongs to
   */
  String getPartitionClass();
}
