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
import org.json.JSONException;


/**
 * A factory that creates a {@link CompositeClusterManager}
 */
public class CompositeClusterManagerFactory implements ClusterManagerFactory {
  private final StaticClusterManagerFactory staticClusterManagerFactory;
  private final HelixClusterManagerFactory helixClusterManagerFactory;

  /**
   * Create an instance of this class.
   * @param clusterMapConfig the {@link ClusterMapConfig} to use.
   * @param hardwareLayoutFilePath the path to the hardware layout file.
   * @param partitionLayoutFilePath the path to the partition layout file.
   * @throws JSONException if there is an exception parsing the layout files.
   * @throws IOException if there is an IO error accessing or reading the layout files.
   */
  public CompositeClusterManagerFactory(ClusterMapConfig clusterMapConfig, String hardwareLayoutFilePath,
      String partitionLayoutFilePath) throws JSONException, IOException {
    staticClusterManagerFactory =
        new StaticClusterManagerFactory(clusterMapConfig, hardwareLayoutFilePath, partitionLayoutFilePath);
    helixClusterManagerFactory =
        new HelixClusterManagerFactory(clusterMapConfig, hardwareLayoutFilePath, partitionLayoutFilePath);
  }

  /**
   * Create and return a {@link CompositeClusterManager}.
   * @return the constructed {@link CompositeClusterManager}.
   * @throws Exception if constructing the underlying {@link StaticClusterManager} or the {@link HelixClusterManager}
   * throws an Exception.
   */
  @Override
  public CompositeClusterManager getClusterManager() throws Exception {
    return new CompositeClusterManager(staticClusterManagerFactory.getClusterManager(),
        helixClusterManagerFactory.getClusterManager());
  }
}

