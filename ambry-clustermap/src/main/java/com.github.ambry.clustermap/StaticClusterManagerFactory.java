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
import org.json.JSONObject;

import static com.github.ambry.utils.Utils.*;


/**
 * A class used to create the {@link StaticClusterManager}
 */
public class StaticClusterManagerFactory implements ClusterManagerFactory {
  private final PartitionLayout partitionLayout;

  /**
   * Instantiate an instance of this factory.
   * @param clusterMapConfig the {@link ClusterMapConfig} to use.
   * @param hardwareLayoutFilePath the path to the hardware layout file.
   * @param partitionLayoutFilePath the path to the partition layout file.
   * @throws JSONException if a JSON error is encountered while parsing the layout files.
   * @throws IOException if an I/O error is encountered accessing or reading the layout files.
   */
  public StaticClusterManagerFactory(ClusterMapConfig clusterMapConfig, String hardwareLayoutFilePath,
      String partitionLayoutFilePath) throws JSONException, IOException {
    this(new PartitionLayout(
        new HardwareLayout(new JSONObject(readStringFromFile(hardwareLayoutFilePath)), clusterMapConfig),
        new JSONObject(readStringFromFile(partitionLayoutFilePath))));
  }

  /**
   * Instantiate an instance of this factory.
   * @param partitionLayout the {@link PartitionLayout} to use.
   */
  StaticClusterManagerFactory(PartitionLayout partitionLayout) {
    this.partitionLayout = partitionLayout;
  }

  @Override
  public StaticClusterManager getClusterManager() throws Exception {
    return new StaticClusterManager(partitionLayout);
  }
}

