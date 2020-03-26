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
import com.github.ambry.network.LocalRequestResponseChannel.LocalChannelRequest;
import com.github.ambry.utils.Time;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An implementation of {@link NetworkClient} that transmits requests and responses over a loopback network,
 * where they can be dispatched directly to the AmbryRequests handler.
 *
 * This class is not thread safe.
 */
public class LocalNetworkClient implements NetworkClient {
  private final LocalRequestResponseChannel channel;
  private final NetworkMetrics networkMetrics;
  private final Time time;
  private boolean closed = false;
  private final int processorId;
  private static final Logger logger = LoggerFactory.getLogger(LocalNetworkClient.class);

  /**
   * Instantiates a LocalNetworkClient.
   * @param channel the {@link RequestResponseChannel} to send requests to.
   * @param processorId the id used to isolate requests and responses sent across the shared channel.
   * @param networkMetrics the metrics to track the network related metrics
   * @param time The Time instance to use.
   */
  public LocalNetworkClient(LocalRequestResponseChannel channel, int processorId, NetworkMetrics networkMetrics,
      Time time) {
    this.channel = channel;
    this.processorId = processorId;
    this.networkMetrics = networkMetrics;
    this.time = time;
  }

  /**
   * Attempt to send the given requests and poll for responses from the network via the associated channel. Any
   * requests that could not be sent out will be added to a queue. Every time this method is called, it will first
   * attempt sending the requests in the queue (or time them out) and then attempt sending the newly added requests.
   * @param requestInfos the list of {@link RequestInfo} representing the requests that need to be sent out. This
   *                     could be empty.
   * @param requestsToDrop the list of correlation IDs representing the requests that can be dropped by
   *                       closing the connection.
   * @param pollTimeoutMs the poll timeout.
   * @return a list of {@link ResponseInfo} representing the responses received for any requests that were sent out
   * so far.
   * @throws IllegalStateException if the client is closed.
   */
  public List<ResponseInfo> sendAndPoll(List<RequestInfo> requestInfos, Set<Integer> requestsToDrop,
      int pollTimeoutMs) {
    if (closed) {
      throw new IllegalStateException("The client is closed.");
    }

    // TODO: do anything with requestsToDrop?
    // The AmbryRequest sessions run in a thread pool, and each thread knows when the response is back so we can poll it
    long startTime = time.milliseconds();
    try {
      for (RequestInfo requestInfo : requestInfos) {
        // TODO: inefficient to serialize request before sending, better to convert Send to an InputStream
        // that handles the header skipping.
        ByteBuf buffer = LocalRequestResponseChannel.byteBufFromPayload(requestInfo.getRequest());
        channel.sendRequest(new LocalChannelRequest(requestInfo, processorId, new ByteBufInputStream(buffer)));
      }
    } catch (Exception e) {
      logger.error("Received an unexpected error during sendAndPoll(): ", e);
      networkMetrics.networkClientException.inc();
    }

    List<ResponseInfo> responses = channel.receiveResponses(processorId, pollTimeoutMs);
    networkMetrics.networkClientSendAndPollTime.update(time.milliseconds() - startTime, TimeUnit.MILLISECONDS);
    return responses;
  }

  /**
   * Close the LocalNetworkClient and cleanup.
   */
  @Override
  public void close() {
    logger.trace("Closing the LocalNetworkClient");
    closed = true;
  }

  @Override
  public int warmUpConnections(List<DataNodeId> dataNodeIds, int connectionWarmUpPercentagePerDataNode,
      long timeForWarmUp, List<ResponseInfo> responseInfoList) {
    return 0;
  }

  @Override
  public void wakeup() {
    channel.wakeup(processorId);
  }
}
