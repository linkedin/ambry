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


/**
 * A factory class to construct {@link HelixClusterManager}
 */
public class HelixClusterManagerFactory implements ClusterManagerFactory {
  private final ClusterMapConfig clusterMapConfig;
  private final String instanceName;

  /**
   * Construct an object of this factory.
   * @param clusterMapConfig the {@link ClusterMapConfig} associated with this factory.
   * @param hardwareLayoutFilePath unused.
   * @param partitionLayoutFilePath unused.
   */
  public HelixClusterManagerFactory(ClusterMapConfig clusterMapConfig, String hardwareLayoutFilePath,
      String partitionLayoutFilePath) {
    this.clusterMapConfig = clusterMapConfig;
    this.instanceName =
        ClusterMapUtils.getInstanceName(clusterMapConfig.clusterMapHostName, clusterMapConfig.clusterMapPort);
  }

  @Override
  public HelixClusterManager getClusterManager() throws Exception {
    return new HelixClusterManager(clusterMapConfig, instanceName);
  }
}

