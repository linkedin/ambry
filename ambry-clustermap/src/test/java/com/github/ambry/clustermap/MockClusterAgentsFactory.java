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

import com.github.ambry.accountstats.AccountStatsStore;
import com.github.ambry.commons.Callback;
import com.github.ambry.server.AmbryStatsReport;
import com.github.ambry.server.storagestats.AggregatedAccountStorageStats;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class MockClusterAgentsFactory implements ClusterAgentsFactory {
  private final boolean enableSslPorts;
  private final boolean enableHttp2Ports;
  private final int numNodes;
  private final int numMountPointsPerNode;
  private final int numStoresPerMountPoint;
  private MockClusterMap mockClusterMap;
  private List<ClusterParticipant> clusterParticipants;
  private List<String> partitionLeadershipList;

  /**
   * Create {@link MockClusterAgentsFactory} object.
   * @param enableSslPorts disable/enable ssl ports.
   * @param enableHttp2Ports disable/enable http2 ports.
   * @param numNodes number of nodes in the cluster.
   * @param numMountPointsPerNode number of mount points per node.
   * @param numStoresPerMountPoint number of stores per mount point.
   */
  public MockClusterAgentsFactory(boolean enableSslPorts, boolean enableHttp2Ports, int numNodes,
      int numMountPointsPerNode, int numStoresPerMountPoint) {
    this.enableSslPorts = enableSslPorts;
    this.enableHttp2Ports = enableHttp2Ports;
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
    this.enableHttp2Ports = mockClusterMap.enableHttp2Ports;
    this.numMountPointsPerNode = mockClusterMap.numMountPointsPerNode;
    this.numNodes = mockClusterMap.dataNodes.size();
    this.numStoresPerMountPoint = mockClusterMap.partitions.size();
    this.partitionLeadershipList = (partitionLeadershipList == null) ? new ArrayList<>() : partitionLeadershipList;
  }

  @Override
  public MockClusterMap getClusterMap() throws IOException {
    if (mockClusterMap == null) {
      mockClusterMap =
          new MockClusterMap(enableSslPorts, enableHttp2Ports, numNodes, numMountPointsPerNode, numStoresPerMountPoint,
              false, false, null);
    }
    return mockClusterMap;
  }

  @Override
  public List<ClusterParticipant> getClusterParticipants() {
    if (clusterParticipants == null) {
      ClusterParticipant clusterParticipant = new ClusterParticipant() {
        private final Map<StateModelListenerType, PartitionStateChangeListener>
            registeredPartitionStateChangeListeners = new HashMap<>();

        @Override
        public void participate(List<AmbryStatsReport> ambryHealthReports, AccountStatsStore accountStatsStore,
            Callback<AggregatedAccountStorageStats> callback) {
          for (String partitionName : partitionLeadershipList) {
            for (PartitionStateChangeListener partitionStateChangeListener : registeredPartitionStateChangeListeners.values()) {
              partitionStateChangeListener.onPartitionBecomeLeaderFromStandby(partitionName);
            }
          }
        }

        @Override
        public void close() {

        }

        @Override
        public boolean setReplicaSealedState(ReplicaId replicaId, ReplicaSealStatus replicaSealStatus) {
          if (!(replicaId instanceof MockReplicaId)) {
            throw new IllegalArgumentException("Not MockReplicaId");
          }
          MockReplicaId mockReplicaId = (MockReplicaId) replicaId;
          mockReplicaId.setReplicaSealStatus(replicaSealStatus);
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
        public List<String> getPartiallySealedReplicas() {
          return new ArrayList<>();
        }

        @Override
        public List<String> getStoppedReplicas() {
          return new ArrayList<>();
        }

        @Override
        public void registerPartitionStateChangeListener(StateModelListenerType listenerType,
            PartitionStateChangeListener partitionStateChangeListener) {
          registeredPartitionStateChangeListeners.put(listenerType, partitionStateChangeListener);
        }

        @Override
        public Map<StateModelListenerType, PartitionStateChangeListener> getPartitionStateChangeListeners() {
          return registeredPartitionStateChangeListeners;
        }

        @Override
        public ReplicaSyncUpManager getReplicaSyncUpManager() {
          return null;
        }

        @Override
        public boolean updateDataNodeInfoInCluster(ReplicaId replicaId, boolean shouldExist) {
          return false;
        }
      };
      clusterParticipants = new ArrayList<>(Collections.singleton(clusterParticipant));
    }
    return clusterParticipants;
  }

  public void setClusterParticipants(List<ClusterParticipant> clusterParticipants) {
    this.clusterParticipants = clusterParticipants;
  }
}
