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
import java.util.ArrayList;
import java.util.List;


public class MockClusterAgentsFactory implements ClusterAgentsFactory {
  private final boolean enableSslPorts;
  private final int numNodes;
  private final int numMountPointsPerNode;
  private final int numStoresPerMountPoint;
  private MockClusterMap mockClusterMap;
  private ClusterParticipant clusterParticipant;
  private List<String> partitionLeadershipList;

  /**
   * Create {@link MockClusterAgentsFactory} object.
   * @param enableSslPorts disable/enable ssl ports.
   * @param numNodes number of nodes in the cluster.
   * @param numMountPointsPerNode number of mount points per node.
   * @param numStoresPerMountPoint number of stores per mount point.
   */
  public MockClusterAgentsFactory(boolean enableSslPorts, int numNodes, int numMountPointsPerNode,
      int numStoresPerMountPoint) {
    this.enableSslPorts = enableSslPorts;
    this.numNodes = numNodes;
    this.numMountPointsPerNode = numMountPointsPerNode;
    this.numStoresPerMountPoint = numStoresPerMountPoint;
    this.partitionLeadershipList = new ArrayList<>();
  }

  /**
   * Create a {@link MockClusterAgentsFactory} object from the given {@code clustermap}.
   * @param mockClusterMap {@link ClusterMap} object.
   */
  public MockClusterAgentsFactory(MockClusterMap mockClusterMap, List<String> partitionLeadershipList) {
    this.mockClusterMap = mockClusterMap;
    this.enableSslPorts = mockClusterMap.enableSSLPorts;
    this.numMountPointsPerNode = mockClusterMap.numMountPointsPerNode;
    this.numNodes = mockClusterMap.dataNodes.size();
    this.numStoresPerMountPoint = mockClusterMap.partitions.size();
    this.partitionLeadershipList = (partitionLeadershipList == null) ? new ArrayList<>() : partitionLeadershipList;
  }

  @Override
  public MockClusterMap getClusterMap() throws IOException {
    if (mockClusterMap == null) {
      mockClusterMap =
          new MockClusterMap(enableSslPorts, numNodes, numMountPointsPerNode, numStoresPerMountPoint, false);
    }
    return mockClusterMap;
  }

  @Override
  public ClusterParticipant getClusterParticipant() {
    if (clusterParticipant == null) {
      clusterParticipant = new ClusterParticipant() {
        private final List<PartitionStateChangeListener> registeredPartitionStateChangeListeners = new ArrayList<>();

        @Override
        public void participate(List<AmbryHealthReport> ambryHealthReports) {
          for (String partitionName : partitionLeadershipList) {
            for (PartitionStateChangeListener partitionStateChangeListener : registeredPartitionStateChangeListeners) {
              partitionStateChangeListener.onPartitionStateChangeToLeaderFromStandby(partitionName);
            }
          }
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

        @Override
        public boolean setReplicaStoppedState(List<ReplicaId> replicaIds, boolean markStop) {
          for (ReplicaId replicaId : replicaIds) {
            if (!(replicaId instanceof MockReplicaId)) {
              throw new IllegalArgumentException("Not MockReplicaId");
            }
            MockReplicaId mockReplicaId = (MockReplicaId) replicaId;
            mockReplicaId.markReplicaDownStatus(markStop);
          }
          return true;
        }

        @Override
        public List<String> getSealedReplicas() {
          return new ArrayList<>();
        }

        @Override
        public List<String> getStoppedReplicas() {
          return new ArrayList<>();
        }

        @Override
        public void registerPartitionStateChangeListener(PartitionStateChangeListener partitionStateChangeListener) {
          registeredPartitionStateChangeListeners.add(partitionStateChangeListener);
        }
      };
    }
    return clusterParticipant;
  }
}
