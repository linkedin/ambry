/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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

import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.server.AmbryHealthReport;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A factory class to construct {@link RecoveryTestClusterManager} and a no op {@link ClusterParticipant}.
 * Only one instance of each type of objects will ever be created by this factory.
 */
public class RecoveryTestClusterAgentsFactory implements ClusterAgentsFactory {
  private static final Logger logger = LoggerFactory.getLogger(RecoveryTestClusterAgentsFactory.class);
  private final StaticClusterAgentsFactory staticClusterAgentsFactory;
  private final HelixClusterAgentsFactory helixClusterAgentsFactory;
  private final AtomicReference<RecoveryTestClusterManager> recoveryTestClusterManagerRef = new AtomicReference<>();
  private final AtomicReference<ClusterParticipant> clusterParticipantRef = new AtomicReference<>();

  /**
   * Create an instance of this class.
   * @param clusterMapConfig the {@link ClusterMapConfig} to use.
   * @param hardwareLayoutFilePath the path to the hardware layout file.
   * @param partitionLayoutFilePath the path to the partition layout file.
   * @throws JSONException if there is an exception parsing the layout files.
   */
  public RecoveryTestClusterAgentsFactory(ClusterMapConfig clusterMapConfig, String hardwareLayoutFilePath,
      String partitionLayoutFilePath) throws JSONException {
    PartitionLayout partitionLayout =
        new PartitionLayout(new HardwareLayout(clusterMapConfig.clustermapRecoveryTestHardwareLayout, clusterMapConfig),
            clusterMapConfig.clustermapRecoveryTestPartitionLayout, clusterMapConfig);
    staticClusterAgentsFactory = new StaticClusterAgentsFactory(clusterMapConfig, partitionLayout);
    helixClusterAgentsFactory =
        new HelixClusterAgentsFactory(clusterMapConfig, staticClusterAgentsFactory.getMetricRegistry());
  }

  /**
   * Create and return a {@link RecoveryTestClusterManager}.
   * @return the constructed {@link RecoveryTestClusterManager}.
   * @throws IOException if constructing the underlying {@link StaticClusterManager} or the {@link HelixClusterManager}
   * throws an Exception.
   */
  @Override
  public RecoveryTestClusterManager getClusterMap() throws IOException {
    if (recoveryTestClusterManagerRef.get() == null) {
      StaticClusterManager staticClusterManager = staticClusterAgentsFactory.getClusterMap();
      HelixClusterManager helixClusterManager = null;
      try {
        helixClusterManager = helixClusterAgentsFactory.getClusterMap();
      } catch (Exception e) {
        logger.error("Helix cluster manager instantiation failed with exception", e);
        throw new IOException(
            String.format("Helix cluster manager instantiation failed with exception %s", e.getMessage()));
      }
      recoveryTestClusterManagerRef.compareAndSet(null,
          new RecoveryTestClusterManager(staticClusterManager, helixClusterManager));
    }
    return recoveryTestClusterManagerRef.get();
  }

  @Override
  public List<ClusterParticipant> getClusterParticipants() throws IOException {
    if (clusterParticipantRef.get() == null) {
      // create a no op cluster participant that does nothing. Just sits idly by!!! ¯\_(ツ)_/¯
      ClusterParticipant clusterParticipant = new ClusterParticipant() {
        @Override
        public void participate(List<AmbryHealthReport> ambryHealthReports) {
        }

        @Override
        public void close() {
        }

        @Override
        public boolean setReplicaSealedState(ReplicaId replicaId, boolean isSealed) {
          return false;
        }

        @Override
        public boolean setReplicaStoppedState(List<ReplicaId> replicaIds, boolean markStop) {
          return false;
        }

        @Override
        public List<String> getSealedReplicas() {
          return Collections.emptyList();
        }

        @Override
        public List<String> getStoppedReplicas() {
          return Collections.emptyList();
        }

        @Override
        public void registerPartitionStateChangeListener(StateModelListenerType listenerType,
            PartitionStateChangeListener partitionStateChangeListener) {
        }

        @Override
        public Map<StateModelListenerType, PartitionStateChangeListener> getPartitionStateChangeListeners() {
          return null;
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
      clusterParticipantRef.compareAndSet(null, clusterParticipant);
    }
    return Collections.singletonList(clusterParticipantRef.get());
  }
}
