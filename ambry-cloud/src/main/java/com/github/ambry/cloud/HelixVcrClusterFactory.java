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

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.AccountService;
import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.VirtualReplicatorCluster;
import com.github.ambry.clustermap.VirtualReplicatorClusterFactory;
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.StoreConfig;


/**
 * {@link HelixVcrClusterFactory} to generate {@link HelixVcrCluster} for dynamic partition assignment.
 */
public class HelixVcrClusterFactory implements VirtualReplicatorClusterFactory {

  private final CloudConfig cloudConfig;
  private final ClusterMapConfig clusterMapConfig;
  private final ClusterMap clusterMap;
  private VirtualReplicatorCluster virtualReplicatorCluster;
  private final AccountService accountService;
  private final StoreConfig storeConfig;
  private final CloudDestination cloudDestination;
  private final VcrMetrics vcrMetrics;

  /**
   * Constructor for {@link HelixVcrClusterFactory}
   * @param cloudConfig {@link CloudConfig} object.
   * @param clusterMapConfig {@link ClusterMapConfig} object.
   * @param clusterMap {@link ClusterMap} object.
   * @param accountService {@link AccountService} object.
   * @param storeConfig {@link StoreConfig} object.
   * @param cloudDestination {@link CloudDestination} object.
   * @param metricRegistry  {@link MetricRegistry} object.
   */
  public HelixVcrClusterFactory(CloudConfig cloudConfig, ClusterMapConfig clusterMapConfig, ClusterMap clusterMap,
      AccountService accountService, StoreConfig storeConfig, CloudDestination cloudDestination,
      MetricRegistry metricRegistry) {
    this.cloudConfig = cloudConfig;
    this.clusterMapConfig = clusterMapConfig;
    this.clusterMap = clusterMap;
    this.accountService = accountService;
    this.storeConfig = storeConfig;
    this.cloudDestination = cloudDestination;
    this.vcrMetrics = new VcrMetrics(metricRegistry);
  }

  @Override
  synchronized public VirtualReplicatorCluster getVirtualReplicatorCluster() throws Exception {
    if (virtualReplicatorCluster == null) {
      virtualReplicatorCluster =
          new HelixVcrCluster(cloudConfig, clusterMapConfig, storeConfig, clusterMap, accountService, cloudDestination,
              vcrMetrics);
    }
    return virtualReplicatorCluster;
  }

  public void close() throws Exception {
    if (virtualReplicatorCluster != null) {
      virtualReplicatorCluster.close();
    }
  }
}
