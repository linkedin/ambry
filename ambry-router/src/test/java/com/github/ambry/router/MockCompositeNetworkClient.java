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
package com.github.ambry.router;

import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.network.CompositeNetworkClient;
import com.github.ambry.network.NetworkClient;
import com.github.ambry.network.RequestInfo;
import com.github.ambry.network.ResponseInfo;
import java.util.List;
import java.util.Set;


/**
 * A mock class used for verifying whether certain methods of the {@link CompositeNetworkClient} gets called in certain
 * tests and how many responses are received by the client.
 */
class MockCompositeNetworkClient implements NetworkClient {
  NetworkClient compNetworkClient;

  /**
   * Construct a MockCompositeNetworkClient with mock components.
   */
  public MockCompositeNetworkClient(NetworkClient client)  {
    compNetworkClient = client;
  }

  /**
   * Wake up the MockNetworkClient. This simply sets a flag that indicates that the method was invoked.
   */
  @Override
  public void wakeup() {
    compNetworkClient.wakeup();
  }

  /**
   * This updates the processed request count along with normal send and poll actions.
   * {@inheritDoc}
   */
  @Override
  public List<ResponseInfo> sendAndPoll(List<RequestInfo> requestsToSend, Set<Integer> requestsToDrop,
      int pollTimeoutMs) {
    List<ResponseInfo> responseInfoList = compNetworkClient.sendAndPoll(requestsToSend, requestsToDrop, pollTimeoutMs);
    return responseInfoList;
  }

  /**
   * Warm up connections to dataNodes in a specified time window.
   * @param dataNodeIds warm up target nodes.
   * @param connectionWarmUpPercentagePerDataNode percentage of max connections would like to establish in the warmup.
   * @param timeForWarmUp max time to wait for connections' establish in milliseconds.
   * @param responseInfoList records responses from disconnected connections.
   * @return number of connections established successfully.
   */
  @Override
  public int warmUpConnections(List<DataNodeId> dataNodeIds, int connectionWarmUpPercentagePerDataNode, long timeForWarmUp,
      List<ResponseInfo> responseInfoList) {
    return compNetworkClient.warmUpConnections(
        dataNodeIds, connectionWarmUpPercentagePerDataNode, timeForWarmUp, responseInfoList);
  }

  @Override
  public void close() {
    compNetworkClient.close();
  }
}

