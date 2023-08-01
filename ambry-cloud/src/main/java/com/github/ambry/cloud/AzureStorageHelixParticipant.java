/**
 * Copyright 2023 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.config.CloudConfig;
import com.github.ambry.config.ClusterMapConfig;
import com.github.ambry.config.StoreConfig;
import com.github.ambry.config.VerifiableProperties;
import org.apache.helix.participant.StateMachineEngine;


/**
 * Helix participant
 */
public class AzureStorageHelixParticipant extends HelixVcrClusterParticipant {

  /**
   * Constructor
   * @param properties Configuration parameters
   * @param registry Metrics
   * @param clusterMap Cluster-map
   * @param accountService Account service
   */
  public AzureStorageHelixParticipant(VerifiableProperties properties, MetricRegistry registry, ClusterMap clusterMap,
      AccountService accountService) {
    super(new CloudConfig(properties), new ClusterMapConfig(properties), new StoreConfig(properties), clusterMap,
        accountService, null, new VcrMetrics(registry));
  }

  @Override
  protected void registerContainerDeletionSyncTask(StateMachineEngine engine) {
    // TODO: Implement and schedule container compactor
  }
}
