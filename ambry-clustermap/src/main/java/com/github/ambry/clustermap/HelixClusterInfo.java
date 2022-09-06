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

import java.util.List;
import org.apache.helix.HelixManager;


public class HelixClusterInfo extends ClusterInfo {

  final HelixManager helixManager;
  private final List<DataNodeConfigSource> dataNodeConfigSources;

  /**
   * Construct a DcInfo object with the given parameters.
   * @param helixManager
   * @param clusterChangeHandler the associated {@link ClusterChangeHandler} for this datacenter.
   * @param dataNodeConfigSources
   */
  HelixClusterInfo(HelixManager helixManager, ClusterChangeHandler clusterChangeHandler,
      List<DataNodeConfigSource> dataNodeConfigSources) {
    super(clusterChangeHandler);
    this.helixManager = helixManager;
    this.dataNodeConfigSources = dataNodeConfigSources;
  }

  @Override
  public void close() {
    try {
      if (helixManager.isConnected()) {
        helixManager.disconnect();
      }
    } finally {
      dataNodeConfigSources.forEach(DataNodeConfigSource::close);
    }
  }
}
