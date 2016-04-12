/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
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
 * A ReplicaId is part of some {@link PartitionId}. The durable state of a ReplicaId is stored in a specific path
 * ("replica path") on a specific device (identified by its "mount path") on a {@link DataNodeId}.
 */
public interface ReplicaId {
  /**
   * Gets the PartitionId of which this ReplicaId is a member.
   *
   * @return PartitionId of which this ReplicaId is a member.
   */
  public PartitionId getPartitionId();

  /**
   * Gets the DataNodeId that stores this ReplicaId.
   *
   * @return DataNodeId that stores this ReplicaId.
   */
  public DataNodeId getDataNodeId();

  /**
   * Gets the absolute path to the mounted device that stores this ReplicaId.
   *
   * @return absolute mount path.
   */
  public String getMountPath();

  /**
   * Gets the absolute path to the directory in which this ReplicaId's files are stored on this DataNodeId. The replica
   * path is the mount path followed by a unique path for this ReplicaId.
   *
   * @return absolute replica path.
   */
  public String getReplicaPath();

  /**
   * Gets list of this ReplicaId's peers. The peers of a ReplicaId are the other Replicas with which this replica forms
   * a PartitionId.
   *
   * @return list of the peers of this ReplicaId.
   */
  public List<ReplicaId> getPeerReplicaIds();

  /**
   * Gets the capacity in bytes for this ReplicaId.
   * @return the capacity in bytes
   */
  public long getCapacityInBytes();

  /**
   * Gets the DiskId that stores this ReplicaId
   * @return DiskId that stores this ReplicaId
   */
  public DiskId getDiskId();

  /**
   * Returns true if the replica is down
   */
  public boolean isDown();
}
