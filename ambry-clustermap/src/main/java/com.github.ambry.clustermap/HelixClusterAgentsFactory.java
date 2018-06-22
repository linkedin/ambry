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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.ClusterMapConfig;
import java.io.IOException;


/**
 * A factory class to construct {@link HelixClusterManager} and {@link HelixParticipant}. Only one instance of each
 * type of objects will ever be created by this factory.
 */
public class HelixClusterAgentsFactory implements ClusterAgentsFactory {
  private final ClusterMapConfig clusterMapConfig;
  private final String instanceName;
  private final HelixFactory helixFactory;
  private final MetricRegistry metricRegistry;
  private HelixClusterManager helixClusterManager;
  private HelixParticipant helixParticipant;

  /**
   * Construct an object of this factory.
   * @param clusterMapConfig the {@link ClusterMapConfig} associated with this factory.
   * @param hardwareLayoutFilePath unused.
   * @param partitionLayoutFilePath unused.
   */
  public HelixClusterAgentsFactory(ClusterMapConfig clusterMapConfig, String hardwareLayoutFilePath,
      String partitionLayoutFilePath) {
    this(clusterMapConfig, new MetricRegistry());
  }

  HelixClusterAgentsFactory(ClusterMapConfig clusterMapConfig, MetricRegistry metricRegistry) {
    this.clusterMapConfig = clusterMapConfig;
    this.instanceName =
        ClusterMapUtils.getInstanceName(clusterMapConfig.clusterMapHostName, clusterMapConfig.clusterMapPort);
    helixFactory = new HelixFactory();
    this.metricRegistry = metricRegistry;
  }

  @Override
  public HelixClusterManager getClusterMap() throws IOException {
    if (helixClusterManager == null) {
      helixClusterManager = new HelixClusterManager(clusterMapConfig, instanceName, helixFactory, metricRegistry);
    }
    return helixClusterManager;
  }

  @Override
  public HelixParticipant getClusterParticipant() throws IOException {
    if (helixParticipant == null) {
      helixParticipant = new HelixParticipant(clusterMapConfig, helixFactory);
    }
    return helixParticipant;
  }
}

