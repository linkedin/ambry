/**
 * Copyright 2016 LinkedIn Corp. All rights reserved.
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
import com.github.ambry.network.ConnectedChannel;
import com.github.ambry.network.ConnectionPool;
import com.github.ambry.network.Port;
import java.util.Map;


/**
 * Implementation of {@link ConnectionPool} that hands out {@link MockConnection}s.
 */
public class MockConnectionPool implements ConnectionPool {
  private final Map<DataNodeId, MockHost> hosts;
  private final ClusterMap clusterMap;
  private final int maxEntriesToReturn;

  public MockConnectionPool(Map<DataNodeId, MockHost> hosts, ClusterMap clusterMap, int maxEntriesToReturn) {
    this.hosts = hosts;
    this.clusterMap = clusterMap;
    this.maxEntriesToReturn = maxEntriesToReturn;
  }

  @Override
  public void start() {
  }

  @Override
  public void shutdown() {
  }

  @Override
  public ConnectedChannel checkOutConnection(String host, Port port, long timeout) {
    DataNodeId dataNodeId = clusterMap.getDataNodeId(host, port.getPort());
    MockHost hostObj = hosts.get(dataNodeId);
    return new MockConnection(hostObj, maxEntriesToReturn);
  }

  @Override
  public void checkInConnection(ConnectedChannel connectedChannel) {
  }

  @Override
  public void destroyConnection(ConnectedChannel connectedChannel) {
  }
}
