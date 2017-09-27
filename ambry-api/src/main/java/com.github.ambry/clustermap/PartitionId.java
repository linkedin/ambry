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
public abstract class PartitionId implements Resource, Comparable<PartitionId> {
  /**
   * Serializes the ID of this PartitionId to bytes.
   *
   * @return byte-serialized ID of this PartitionId.
   */
  public abstract byte[] getBytes();

  /**
   * Gets Replicas that comprise this PartitionId.
   *
   * @return list of the Replicas that comprise this PartitionId.
   */
  public abstract List<? extends ReplicaId> getReplicaIds();

  /**
   * Gets the state of this PartitionId.
   *
   * @return state of this PartitionId.
   */
  public abstract PartitionState getPartitionState();

  /**
   * Compares the PartitionId to a string representation of another PartitionId
   * @param partitionId  The string form of the partition that needs to be compared against
   * @return True, if the partitions match, false otherwise
   */
  public abstract boolean isEqual(String partitionId);

  /**
   * Returns a {@link String} that uniquely represents the {@code PartitionId}.
   * @return String representation of the {@code PartitionId}.
   */
  @Override
  public abstract String toString();

  /**
   * Returns a strictly numerical {@link String} that uniquely represents the {@code PartitionId}.
   * @return Strictly numerical string representation of the {@code PartitionId}.
   */
  public abstract String toPathString();
}
