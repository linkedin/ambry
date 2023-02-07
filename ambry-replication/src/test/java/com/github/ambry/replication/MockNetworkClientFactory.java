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

package com.github.ambry.replication;

import com.github.ambry.clustermap.ClusterMap;
import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.network.NetworkClient;
import com.github.ambry.network.NetworkClientFactory;
import java.io.IOException;
import java.util.Map;


public class MockNetworkClientFactory implements NetworkClientFactory {

  private final Map<DataNodeId, MockHost> hosts;
  private final ClusterMap clusterMap;
  private final FindTokenHelper findTokenHelper;
  private final int batchSize;

  public MockNetworkClientFactory(Map<DataNodeId, MockHost> hosts, ClusterMap clusterMap, int batchSize,
      FindTokenHelper findTokenHelper) {
    this.batchSize = batchSize;
    this.clusterMap = clusterMap;
    this.findTokenHelper = findTokenHelper;
    this.hosts = hosts;
  }

  @Override
  public NetworkClient getNetworkClient() throws IOException {
    return new MockNetworkClient(hosts, clusterMap, batchSize, findTokenHelper);
  }
}
