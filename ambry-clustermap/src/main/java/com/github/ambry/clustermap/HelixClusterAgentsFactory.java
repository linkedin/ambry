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
import java.util.ArrayList;
import java.util.List;
import org.json.JSONException;


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
  private List<ClusterParticipant> helixParticipants;

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

  /**
   * Ctor exposed for testing purpose
   * @param clusterMapConfig the {@link ClusterMapConfig} to specify cluster configuration parameters.
   * @param helixFactory the {@link HelixFactory} that helps get reference of HelixManager and HelixAdmin.
   */
  HelixClusterAgentsFactory(ClusterMapConfig clusterMapConfig, HelixFactory helixFactory) {
    this.clusterMapConfig = clusterMapConfig;
    this.instanceName =
        ClusterMapUtils.getInstanceName(clusterMapConfig.clusterMapHostName, clusterMapConfig.clusterMapPort);
    this.helixFactory = helixFactory;
    this.metricRegistry = new MetricRegistry();
  }

  @Override
  public HelixClusterManager getClusterMap() throws IOException {
    if (helixClusterManager == null) {
      helixClusterManager = new HelixClusterManager(clusterMapConfig, instanceName, helixFactory, metricRegistry);
    }
    return helixClusterManager;
  }

  @Override
  public List<ClusterParticipant> getClusterParticipants() throws IOException {
    if (helixParticipants == null) {
      helixParticipants = new ArrayList<>();
      try {
        List<String> zkConnectStrs =
            ClusterMapUtils.parseDcJsonAndPopulateDcInfo(clusterMapConfig.clusterMapDcsZkConnectStrings)
                .get(clusterMapConfig.clusterMapDatacenterName)
                .getZkConnectStrs();
        boolean isSoleParticipant = zkConnectStrs.size() == 1;
        for (String zkConnectStr : zkConnectStrs) {
          helixParticipants.add(
              new HelixParticipant(clusterMapConfig, helixFactory, metricRegistry, zkConnectStr, isSoleParticipant));
        }
      } catch (JSONException e) {
        throw new IOException("Received JSON exception while parsing ZKInfo json string", e);
      }
    }
    return helixParticipants;
  }
}

