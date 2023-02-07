/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
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
 *
 */

package com.github.ambry.clustermap;

import com.github.ambry.clustermap.HelixClusterManager.HelixClusterChangeHandler;
import org.apache.helix.HelixManager;
import org.apache.helix.spectator.RoutingTableProvider;


/**
 * Extension of {@link DcInfo} to include a reference the {@link HelixManager} of the datacenter for orderly shutdown
 * and testing.
 */
class HelixDcInfo extends DcInfo {
  final HelixManager helixManager;
  private final DataNodeConfigSource dataNodeConfigSource;
  private RoutingTableProvider routingTableProvider;

  /**
   * Construct a HelixDcInfo object with the given parameters.
   *
   * @param dcName               the associated datacenter name.
   * @param dcZkInfo             the {@link ClusterMapUtils.DcZkInfo} associated with the DC.
   * @param helixManager         the associated {@link HelixManager} for this datacenter. This can be null if the
   *                             datacenter is not managed by helix.
   * @param clusterChangeHandler the associated {@link HelixClusterChangeHandler} for this datacenter.
   * @param dataNodeConfigSource the {@link DataNodeConfigSource} for this datacenter.
   * @param routingTableProvider a helix helper class to provide snapshot of the cluster.
   */
  HelixDcInfo(String dcName, ClusterMapUtils.DcZkInfo dcZkInfo, HelixManager helixManager,
      HelixClusterChangeHandler clusterChangeHandler, DataNodeConfigSource dataNodeConfigSource,
      RoutingTableProvider routingTableProvider) {
    super(dcName, dcZkInfo, clusterChangeHandler);
    this.helixManager = helixManager;
    this.dataNodeConfigSource = dataNodeConfigSource;
    this.routingTableProvider = routingTableProvider;
  }

  @Override
  public void close() {
    try {
      if (helixManager.isConnected()) {
        helixManager.disconnect();
      }
    } finally {
      dataNodeConfigSource.close();
      routingTableProvider.shutdown();
    }
  }
}
