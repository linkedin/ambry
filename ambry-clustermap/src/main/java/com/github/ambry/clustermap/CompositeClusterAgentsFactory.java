/*
 * Copyright 2017 LinkedIn Corp. All rights reserved.
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
import java.util.List;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A factory that creates a {@link CompositeClusterManager} and {@link ClusterParticipant}. Only one instance of each
 * type of objects will ever be created by this factory.
 */
public class CompositeClusterAgentsFactory implements ClusterAgentsFactory {
  private static final Logger logger = LoggerFactory.getLogger(CompositeClusterAgentsFactory.class);
  private final StaticClusterAgentsFactory staticClusterAgentsFactory;
  private final HelixClusterAgentsFactory helixClusterAgentsFactory;
  private CompositeClusterManager compositeClusterManager;
  private List<ClusterParticipant> clusterParticipants;

  /**
   * Create an instance of this class.
   * @param clusterMapConfig the {@link ClusterMapConfig} to use.
   * @param hardwareLayoutFilePath the path to the hardware layout file.
   * @param partitionLayoutFilePath the path to the partition layout file.
   * @throws JSONException if there is an exception parsing the layout files.
   * @throws IOException if there is an IO error accessing or reading the layout files.
   */
  public CompositeClusterAgentsFactory(ClusterMapConfig clusterMapConfig, String hardwareLayoutFilePath,
      String partitionLayoutFilePath) throws JSONException, IOException {
    staticClusterAgentsFactory =
        new StaticClusterAgentsFactory(clusterMapConfig, hardwareLayoutFilePath, partitionLayoutFilePath);
    helixClusterAgentsFactory =
        new HelixClusterAgentsFactory(clusterMapConfig, staticClusterAgentsFactory.getMetricRegistry());
  }

  /**
   * Create an instance of this class.
   * @param clusterMapConfig the {@link ClusterMapConfig} to use.
   * @param partitionLayout the {@link PartitionLayout} to use with the internal {@link StaticClusterManager}
   * @throws JSONException if there is an exception parsing the layout files.
   * @throws IOException if there is an IO error accessing or reading the layout files.
   */
  CompositeClusterAgentsFactory(ClusterMapConfig clusterMapConfig, PartitionLayout partitionLayout)
      throws JSONException, IOException {
    staticClusterAgentsFactory = new StaticClusterAgentsFactory(clusterMapConfig, partitionLayout);
    helixClusterAgentsFactory =
        new HelixClusterAgentsFactory(clusterMapConfig, staticClusterAgentsFactory.getMetricRegistry());
  }

  /**
   * Create and return a {@link CompositeClusterManager}.
   * @return the constructed {@link CompositeClusterManager}.
   * @throws Exception if constructing the underlying {@link StaticClusterManager} or the {@link HelixClusterManager}
   * throws an Exception.
   */
  @Override
  public CompositeClusterManager getClusterMap() throws IOException {
    if (compositeClusterManager == null) {
      StaticClusterManager staticClusterManager = staticClusterAgentsFactory.getClusterMap();
      HelixClusterManager helixClusterManager = null;
      try {
        helixClusterManager = helixClusterAgentsFactory.getClusterMap();
      } catch (Exception e) {
        logger.error("Helix cluster manager instantiation failed with exception", e);
      }
      compositeClusterManager = new CompositeClusterManager(staticClusterManager, helixClusterManager);
    }
    return compositeClusterManager;
  }

  @Override
  public List<ClusterParticipant> getClusterParticipants() throws IOException {
    if (clusterParticipants == null) {
      clusterParticipants = helixClusterAgentsFactory.getClusterParticipants();
    }
    return clusterParticipants;
  }
}

