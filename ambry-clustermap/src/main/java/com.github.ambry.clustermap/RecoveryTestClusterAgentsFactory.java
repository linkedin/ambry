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
import java.io.IOException;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 */
public class RecoveryTestClusterAgentsFactory implements ClusterAgentsFactory {
  private static final Logger logger = LoggerFactory.getLogger(CompositeClusterAgentsFactory.class);
  private final StaticClusterAgentsFactory staticClusterAgentsFactory;
  private final HelixClusterAgentsFactory helixClusterAgentsFactory;
  private RecoveryTestClusterManager recoveryTestClusterManager;
  private ClusterParticipant clusterParticipant;

  /**
   * Create an instance of this class.
   * @param clusterMapConfig the {@link ClusterMapConfig} to use.
   * @param hardwareLayoutFilePath the path to the hardware layout file.
   * @param partitionLayoutFilePath the path to the partition layout file.
   * @throws JSONException if there is an exception parsing the layout files.
   * @throws IOException if there is an IO error accessing or reading the layout files.
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
   * @throws Exception if constructing the underlying {@link StaticClusterManager} or the {@link HelixClusterManager}
   * throws an Exception.
   */
  @Override
  public RecoveryTestClusterManager getClusterMap() throws IOException {
    if (recoveryTestClusterManager == null) {
      StaticClusterManager staticClusterManager = staticClusterAgentsFactory.getClusterMap();
      HelixClusterManager helixClusterManager = null;
      try {
        helixClusterManager = helixClusterAgentsFactory.getClusterMap();
      } catch (Exception e) {
        logger.error("Helix cluster manager instantiation failed with exception", e);
      }
      recoveryTestClusterManager = new RecoveryTestClusterManager(staticClusterManager, helixClusterManager);
    }
    return recoveryTestClusterManager;
  }

  @Override
  public ClusterParticipant getClusterParticipant() throws IOException {
    if (clusterParticipant == null) {
      clusterParticipant = helixClusterAgentsFactory.getClusterParticipant();
    }
    return clusterParticipant;
  }
}
