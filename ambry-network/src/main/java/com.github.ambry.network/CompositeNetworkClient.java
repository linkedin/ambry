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

package com.github.ambry.network;

import com.github.ambry.clustermap.DataNodeId;
import com.github.ambry.clustermap.ReplicaType;
import com.github.ambry.utils.Pair;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;


public class CompositeNetworkClient implements NetworkClient {
  /**
   * Used to properly route requests to drop.
   */
  private final Map<Integer, ReplicaType> correlationIdToReplicaType = new HashMap<>();
  private final EnumMap<ReplicaType, NetworkClient> childNetworkClients;

  CompositeNetworkClient(EnumMap<ReplicaType, NetworkClient> childNetworkClients) {
    this.childNetworkClients = childNetworkClients;
  }

  @Override
  public List<ResponseInfo> sendAndPoll(List<RequestInfo> allRequestsToSend, Set<Integer> allRequestsToDrop,
      int pollTimeoutMs) {
    // the first item of the pair is requests to send, the second is correlation IDs to drop
    Function<ReplicaType, Pair<List<RequestInfo>, Set<Integer>>> pairBuilder =
        replicaType -> new Pair<>(new ArrayList<>(), new HashSet<>());
    EnumMap<ReplicaType, Pair<List<RequestInfo>, Set<Integer>>> requestsToSendAndDropByType =
        new EnumMap<>(ReplicaType.class);
    for (RequestInfo requestInfo : allRequestsToSend) {
      ReplicaType replicaType = requestInfo.getReplicaId().getReplicaType();
      requestsToSendAndDropByType.computeIfAbsent(replicaType, pairBuilder).getFirst().add(requestInfo);
      correlationIdToReplicaType.put(requestInfo.getRequest().getCorrelationId(), replicaType);
    }
    for (Integer correlationId : allRequestsToDrop) {
      ReplicaType replicaType = correlationIdToReplicaType.get(correlationId);
      if (replicaType != null) {
        requestsToSendAndDropByType.computeIfAbsent(replicaType, pairBuilder).getSecond().add(correlationId);
      }
    }
    List<ResponseInfo> responses = new ArrayList<>();
    requestsToSendAndDropByType.forEach((replicaType, requestsToSendAndDrop) -> {
      NetworkClient client = childNetworkClients.get(replicaType);
      if (client == null) {
        throw new IllegalStateException("No NetworkClient configured for replica type: " + replicaType);
      }
      responses.addAll(
          client.sendAndPoll(requestsToSendAndDrop.getFirst(), requestsToSendAndDrop.getSecond(), pollTimeoutMs));
    });
    // clean up correlation ids for completed requests
    responses.forEach(
        response -> correlationIdToReplicaType.remove(response.getRequestInfo().getRequest().getCorrelationId()));
    return responses;
  }

  @Override
  public int warmUpConnections(List<DataNodeId> dataNodeIds, int connectionWarmUpPercentagePerDataNode,
      long timeForWarmUp, List<ResponseInfo> responseInfoList) {
    // currently warm-up is only supported for connections to ambry-server.
    NetworkClient client = childNetworkClients.get(ReplicaType.DISK_BACKED);
    if (client != null) {
      return client.warmUpConnections(dataNodeIds, connectionWarmUpPercentagePerDataNode, timeForWarmUp,
          responseInfoList);
    } else {
      return 0;
    }
  }

  @Override
  public void wakeup() {
    childNetworkClients.values().forEach(NetworkClient::wakeup);
  }

  @Override
  public void close() {
    childNetworkClients.values().forEach(NetworkClient::close);
  }
}
