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

import com.github.ambry.server.AmbryHealthReport;
import java.io.IOException;
import java.util.List;


public class MockClusterAgentsFactory implements ClusterAgentsFactory {
  private final boolean enableSslPorts;
  private final int numNodes;
  private final int numMountPointsPerNode;
  private final int numStoresPerMountPoint;
  private MockClusterMap mockClusterMap;
  private ClusterParticipant clusterParticipant;

  public MockClusterAgentsFactory(boolean enableSssPorts, int numNodes, int numMountPointsPerNode,
      int numStoresPerMountPoint) {
    this.enableSslPorts = enableSssPorts;
    this.numNodes = numNodes;
    this.numMountPointsPerNode = numMountPointsPerNode;
    this.numStoresPerMountPoint = numStoresPerMountPoint;
  }

  @Override
  public MockClusterMap getClusterMap() throws IOException {
    if (mockClusterMap == null) {
      mockClusterMap = new MockClusterMap(enableSslPorts, numNodes, numMountPointsPerNode, numStoresPerMountPoint);
    }
    return mockClusterMap;
  }

  @Override
  public ClusterParticipant getClusterParticipant() throws IOException {
    if (clusterParticipant == null) {
      clusterParticipant = new ClusterParticipant() {
        @Override
        public void initialize(String hostname, int port, List<AmbryHealthReport> ambryHealthReports) {

        }

        @Override
        public void close() {

        }

        @Override
        public boolean setReplicaSealedState(ReplicaId replicaId, boolean isSealed) {
          if (!(replicaId instanceof MockReplicaId)) {
            throw new IllegalArgumentException("Not MockReplicaId");
          }
          MockReplicaId mockReplicaId = (MockReplicaId) replicaId;
          mockReplicaId.setSealedState(isSealed);
          return true;
        }
      };
    }
    return clusterParticipant;
  }
}
