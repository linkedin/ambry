/**
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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
package com.github.ambry.cloud;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.VirtualReplicatorCluster;
import com.github.ambry.clustermap.VirtualReplicatorClusterFactory;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;


/**
 * {@link HelixVcrClusterFactory} to generate {@link HelixVcrCluster} for dynamic partition assignment.
 */
public class HelixVcrClusterFactory implements VirtualReplicatorClusterFactory {

  private final CloudConfig cloudConfig;
  private final ClusterMapConfig clusterMapConfig;
  private final ClusterMap clusterMap;
  private VirtualReplicatorCluster virtualReplicatorCluster;

  public HelixVcrClusterFactory(CloudConfig cloudConfig, ClusterMapConfig clusterMapConfig, ClusterMap clusterMap) {
    this.cloudConfig = cloudConfig;
    this.clusterMapConfig = clusterMapConfig;
    this.clusterMap = clusterMap;
  }

  @Override
  synchronized public VirtualReplicatorCluster getVirtualReplicatorCluster() throws Exception {
    if (virtualReplicatorCluster == null) {
      virtualReplicatorCluster = new HelixVcrCluster(cloudConfig, clusterMapConfig, clusterMap);
    }
    return virtualReplicatorCluster;
  }

  public void close() throws Exception {
    if (virtualReplicatorCluster != null) {
      virtualReplicatorCluster.close();
    }
  }
}
