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
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * An implementation of {@link NetworkClient} that routes requests to child clients based on the {@link ReplicaType}
 * in the provided requests.
 */
public class CompositeNetworkClient implements NetworkClient {
  /**
   * Used to properly route requests to drop.
   */
  private final Map<Integer, ReplicaType> correlationIdToReplicaType = new HashMap<>();
  private final EnumMap<ReplicaType, NetworkClient> childNetworkClients;

  /**
   * @param childNetworkClients a map from {@link ReplicaType} to {@link NetworkClient} to use for that type.
   */
  CompositeNetworkClient(EnumMap<ReplicaType, NetworkClient> childNetworkClients) {
    this.childNetworkClients = childNetworkClients;
  }

  @Override
  public List<ResponseInfo> sendAndPoll(List<RequestInfo> allRequestsToSend, Set<Integer> allRequestsToDrop,
      int pollTimeoutMs) {
    EnumMap<ReplicaType, List<RequestInfo>> requestsToSendByType = new EnumMap<>(ReplicaType.class);
    EnumMap<ReplicaType, Set<Integer>> requestsToDropByType = new EnumMap<>(ReplicaType.class);
    for (ReplicaType replicaType : childNetworkClients.keySet()) {
      requestsToSendByType.put(replicaType, new ArrayList<>());
      requestsToDropByType.put(replicaType, new HashSet<>());
    }
    for (RequestInfo requestInfo : allRequestsToSend) {
      ReplicaType replicaType = requestInfo.getReplicaId().getReplicaType();
      List<RequestInfo> requestsToSend = requestsToSendByType.get(replicaType);
      if (requestsToSend == null) {
        throw new IllegalStateException("No NetworkClient configured for replica type: " + replicaType);
      }
      requestsToSend.add(requestInfo);
      correlationIdToReplicaType.put(requestInfo.getRequest().getCorrelationId(), replicaType);
    }
    for (Integer correlationId : allRequestsToDrop) {
      ReplicaType replicaType = correlationIdToReplicaType.get(correlationId);
      if (replicaType != null) {
        requestsToDropByType.get(replicaType).add(correlationId);
      }
    }
    List<ResponseInfo> responses = new ArrayList<>();
    childNetworkClients.forEach((replicaType, client) -> {
      List<RequestInfo> requestsToSend = requestsToSendByType.get(replicaType);
      Set<Integer> requestsToDrop = requestsToDropByType.get(replicaType);
      responses.addAll(client.sendAndPoll(requestsToSend, requestsToDrop, pollTimeoutMs));
    });
    // clean up correlation ids for completed requests
    responses.forEach(
        response -> correlationIdToReplicaType.remove(response.getRequestInfo().getRequest().getCorrelationId()));
    return responses;
  }

  @Override
  public int warmUpConnections(List<DataNodeId> dataNodeIds, int connectionWarmUpPercentagePerDataNode,
      long timeForWarmUp, List<ResponseInfo> responseInfoList) {
    return childNetworkClients.values()
        .stream()
        .mapToInt(client -> client.warmUpConnections(dataNodeIds, connectionWarmUpPercentagePerDataNode, timeForWarmUp,
            responseInfoList))
        .sum();
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
