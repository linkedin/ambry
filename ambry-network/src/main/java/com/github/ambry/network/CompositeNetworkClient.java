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
import com.github.ambry.utils.Utils;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An implementation of {@link NetworkClient} that routes requests to child clients based on the {@link ReplicaType}
 * in the provided requests.
 */
public class CompositeNetworkClient implements NetworkClient {
  private static final Logger logger = LoggerFactory.getLogger(CompositeNetworkClient.class);
  /**
   * Used to properly route requests to drop.
   */
  private final Map<Integer, ReplicaType> correlationIdToReplicaType = new HashMap<>();
  private final EnumMap<ReplicaType, NetworkClient> childNetworkClients;
  private final ExecutorService executor;

  /**
   * @param childNetworkClients a map from {@link ReplicaType} to {@link NetworkClient} to use for that type.
   */
  CompositeNetworkClient(EnumMap<ReplicaType, NetworkClient> childNetworkClients) {
    this.childNetworkClients = childNetworkClients;
    this.executor = Executors.newFixedThreadPool(childNetworkClients.size());
  }

  @Override
  public List<ResponseInfo> sendAndPoll(List<RequestInfo> allRequestsToSend, Set<Integer> allRequestsToDrop,
      int pollTimeoutMs) {
    // prepare lists of requests to send and drop
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

    // send requests using child clients from background threads so that inactive clients do not block the other client
    // from making progress.
    AtomicBoolean wakeupCalled = new AtomicBoolean(false);
    ArrayList<Future<List<ResponseInfo>>> sendAndPollFutures = new ArrayList<>(childNetworkClients.size());
    childNetworkClients.forEach((replicaType, client) -> {
      List<RequestInfo> requestsToSend = requestsToSendByType.get(replicaType);
      Set<Integer> requestsToDrop = requestsToDropByType.get(replicaType);
      if (!requestsToSend.isEmpty() || !requestsToDrop.isEmpty()) {
        logger.trace("replicaType={}, requestsToSend={}, requestsToDrop={}", replicaType, requestsToSend,
            requestsToDrop);
      }
      sendAndPollFutures.add(executor.submit(() -> {
        List<ResponseInfo> childClientResponses = client.sendAndPoll(requestsToSend, requestsToDrop, pollTimeoutMs);
        if (wakeupCalled.compareAndSet(false, true)) {
          // the client that gets a response first can wake up the other clients so that they do not waste time waiting
          // for the poll timeout to expire. This helps when one child client is very active and the others have very
          // little activity.
          childNetworkClients.values().stream().filter(c -> c != client).forEach(NetworkClient::wakeup);
        }
        return childClientResponses;
      }));
    });

    // process responses returned by each child client
    List<ResponseInfo> responses = new ArrayList<>();
    for (Future<List<ResponseInfo>> future : sendAndPollFutures) {
      try {
        List<ResponseInfo> responseInfoList = future.get();
        for (ResponseInfo responseInfo : responseInfoList) {
          // clean up correlation ids for completed requests
          if (responseInfo.getRequestInfo() != null) {
            correlationIdToReplicaType.remove(responseInfo.getRequestInfo().getRequest().getCorrelationId());
          }
          responses.add(responseInfo);
        }
      } catch (InterruptedException | ExecutionException e) {
        logger.error("Hit unexpected exception on parallel sendAndPoll.", e);
      }
    }
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
    Utils.shutDownExecutorService(executor, 1, TimeUnit.MINUTES);
  }
}
