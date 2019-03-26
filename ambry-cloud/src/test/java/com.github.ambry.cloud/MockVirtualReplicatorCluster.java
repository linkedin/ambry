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
package com.github.ambry.cloud;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.PartitionId;
import com.github.ambry.clustermap.VirtualReplicatorCluster;
import java.util.List;


/**
 * A mock implementation for {@link VirtualReplicatorCluster}.
 */
public class MockVirtualReplicatorCluster implements VirtualReplicatorCluster {

  DataNodeId dataNodeId;
  ClusterMap clusterMap;

  /**
   * Instantiate {@link MockVirtualReplicatorCluster}.
   * @param dataNodeId the {@link DataNodeId} representation of current VCR.
   * @param clusterMap the {@link ClusterMap} of dataNodes cluster.
   */
  public MockVirtualReplicatorCluster(DataNodeId dataNodeId, ClusterMap clusterMap) {
    this.dataNodeId = dataNodeId;
    this.clusterMap = clusterMap;
  }

  @Override
  public List<? extends DataNodeId> getAllDataNodeIds() {
    return null;
  }

  @Override
  public DataNodeId getCurrentDataNodeId() {
    return dataNodeId;
  }

  @Override
  public List<? extends PartitionId> getAssignedPartitionIds() {
    return clusterMap.getAllPartitionIds(null);
  }

  @Override
  public void close() throws Exception {

  }
}
