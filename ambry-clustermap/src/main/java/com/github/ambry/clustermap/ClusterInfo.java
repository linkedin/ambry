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
import java.io.Closeable;


/**
 * Class that stores all the information associated with a cluster.
 */
class ClusterInfo implements Closeable {

  // Handler that handles any resource state changes in the entire cluster
  final HelixClusterChangeHandler clusterChangeHandler;

  /**
   * Construct a ClusterInfo object with the given parameters.
   * @param clusterChangeHandler the associated {@link HelixClusterChangeHandler}
   *                            for this datacenter.
   *
   */
  ClusterInfo(HelixClusterChangeHandler clusterChangeHandler) {
    this.clusterChangeHandler = clusterChangeHandler;
  }

  @Override
  public void close() {
  }
}
