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
package com.github.ambry.network;

import com.github.ambry.clustermap.DataNodeId;
import java.io.Closeable;
import java.util.List;
import java.util.Set;


/**
 * A {@link NetworkClient}  provides a method for sending a list of requests to one or more destinations,
 * and receiving responses for sent requests.
 */
public interface NetworkClient extends Closeable {

  /**
   * Attempt to send the given requests and poll for responses from the network.
   * Any requests that cannot be sent out are added to a queue. Every time this method is called, it will first
   * attempt sending the requests in the queue (or time them out) and then attempt sending the newly added requests.
   * @param requestsToSend the list of {@link RequestInfo} representing the requests that need to be sent out. This
   *                     could be empty.
   * @param requestsToDrop the list of correlation IDs representing the requests that can be dropped by
   *                       closing the connection.
   * @param pollTimeoutMs the poll timeout.
   * @return a list of {@link ResponseInfo} representing the responses received for any requests that were sent out
   * so far.
   * @throws IllegalStateException if the NetworkClient is closed.
   */
  List<ResponseInfo> sendAndPoll(List<RequestInfo> requestsToSend, Set<Integer> requestsToDrop, int pollTimeoutMs);

  /**
   * Warm up connections to dataNodes in a specified time window.
   * @param dataNodeIds warm up target nodes.
   * @param connectionWarmUpPercentagePerDataNode percentage of max connections would like to establish in the warmup.
   * @param timeForWarmUp max time to wait for connections' establish in milliseconds.
   * @param responseInfoList records responses from disconnected connections.
   * @return number of connections established successfully.
   */
  int warmUpConnections(List<DataNodeId> dataNodeIds, int connectionWarmUpPercentagePerDataNode, long timeForWarmUp,
      List<ResponseInfo> responseInfoList);

  /**
   * Wake up the NetworkClient if it is within a {@link #sendAndPoll(List, Set, int)} sleep.
   */
  void wakeup();

  @Override
  void close();
}
