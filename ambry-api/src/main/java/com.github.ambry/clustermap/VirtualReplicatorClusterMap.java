/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
 * The {@link VirtualReplicatorClusterMap} provides a high-level interface to Virtual Replicator Cluster.
 * In Virtual Replicator Cluster, {@link PartitionId}s are resources and they are assigned to virtual replicators.
 */
public interface VirtualReplicatorClusterMap extends AutoCloseable {

  /**
   * Gets a specific DataNodeId by its hostname and port.
   *
   * @param hostname of the DataNodeId
   * @param port of the DataNodeId
   * @return DataNodeId for this hostname and port.
   */
  DataNodeId getDataNodeId(String hostname, int port);

  /**
   * Gets all PartitionIds backing up by given DataNodeId.
   *
   * @param dataNodeId the {@link DataNodeId} whose replicas are to be returned.
   * @return list of PartitionId on the specified dataNodeId
   */
  List<? extends PartitionId> getPartitionIds(DataNodeId dataNodeId);
}
