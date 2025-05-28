/**
 * Copyright 2025 LinkedIn Corp. All rights reserved.
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

package com.github.ambry.server;

import com.github.ambry.clustermap.ClusterMapChangeListener;
import com.github.ambry.clustermap.DataNodeConfig;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Implementation of {@link ClusterMapChangeListener} that is used to notify the server when the data node config
 * changes in the cluster map.
 */
public class AmbryServerClusterMapChangeListenerImpl implements ClusterMapChangeListener {
  private final String localInstanceName;
  private final CountDownLatch dataNodeLatch;

  /**
   * Constructor for AmbryServerClusterMapChangeListenerImpl.
   * @param localInstanceName the current node instanceName
   * @param dataNodeLatch the latch to be counted down when the data node config changes.
   */
  AmbryServerClusterMapChangeListenerImpl(String localInstanceName, CountDownLatch dataNodeLatch) {
    this.localInstanceName = localInstanceName;
    this.dataNodeLatch = dataNodeLatch;
  }

  /**
   * This method is called when the dataNodeConfigs changes in cluster map.
   * @param configs the list of {@link DataNodeConfig} that changed.
   */
  @Override
  public void onDataNodeConfigChange(List<DataNodeConfig> configs) {
    for (DataNodeConfig currNodeConfig : configs) {
      String instanceName = currNodeConfig.getInstanceName();
      if (instanceName.equals(localInstanceName)) {
        dataNodeLatch.countDown();
        break;
      }
    }
  }
}
