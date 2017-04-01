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
import org.json.JSONException;
import org.json.JSONObject;

import static com.github.ambry.utils.Utils.*;


/**
 * A class used to create the {@link StaticClusterManager} and {@link ClusterParticipant}. Only one instance of each
 * type of objects will ever be created by this factory.
 */
public class StaticClusterAgentsFactory implements ClusterAgentsFactory {
  private final PartitionLayout partitionLayout;
  private final ClusterMapConfig clusterMapConfig;
  private final MetricRegistry metricRegistry;
  private StaticClusterManager staticClusterManager;
  private ClusterParticipant clusterParticipant;

  /**
   * Instantiate an instance of this factory.
   * @param clusterMapConfig the {@link ClusterMapConfig} to use.
   * @param hardwareLayoutFilePath the path to the hardware layout file.
   * @param partitionLayoutFilePath the path to the partition layout file.
   * @throws JSONException if a JSON error is encountered while parsing the layout files.
   * @throws IOException if an I/O error is encountered accessing or reading the layout files.
   */
  public StaticClusterAgentsFactory(ClusterMapConfig clusterMapConfig, String hardwareLayoutFilePath,
      String partitionLayoutFilePath) throws JSONException, IOException {
    this(clusterMapConfig, new PartitionLayout(
        new HardwareLayout(new JSONObject(readStringFromFile(hardwareLayoutFilePath)), clusterMapConfig),
        new JSONObject(readStringFromFile(partitionLayoutFilePath))));
  }

  /**
   * Instantiate an instance of this factory.
   * @param partitionLayout the {@link PartitionLayout} to use.
   */
  StaticClusterAgentsFactory(ClusterMapConfig clusterMapConfig, PartitionLayout partitionLayout) {
    this.clusterMapConfig = clusterMapConfig;
    this.partitionLayout = partitionLayout;
    this.metricRegistry = new MetricRegistry();
  }

  @Override
  public StaticClusterManager getClusterMap() {
    if (staticClusterManager == null) {
      staticClusterManager = new StaticClusterManager(partitionLayout, metricRegistry);
    }
    return staticClusterManager;
  }

  @Override
  public ClusterParticipant getClusterParticipant() throws IOException {
    if (clusterParticipant == null) {
      clusterParticipant = new ClusterParticipant() {
        @Override
        public void initialize(String hostname, int port) {

        }

        @Override
        public void close() {

        }
      };
    }
    return clusterParticipant;
  }

  /**
   * @return the {@link MetricRegistry} used when creating the {@link StaticClusterManager}
   */
  MetricRegistry getMetricRegistry() {
    return metricRegistry;
  }
}

