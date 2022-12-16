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

import com.github.ambry.utils.BatchBlockingQueue;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An implementation of {@link RequestResponseChannel} that buffers messages in local queues.
 * This class enables the Frontend router to call AmbryRequest methods in process.
 */
public class LocalRequestResponseChannel implements RequestResponseChannel {

  private static final Logger logger = LoggerFactory.getLogger(LocalRequestResponseChannel.class);
  private BlockingQueue<NetworkRequest> requestQueue = new LinkedBlockingQueue<>();
  private Map<Integer, BatchBlockingQueue<ResponseInfo>> responseMap = new ConcurrentHashMap<>();
  private static final ResponseInfo WAKEUP_MARKER = new ResponseInfo(null, null, null);

  @Override
  public void sendRequest(NetworkRequest request) {
    requestQueue.offer(request);
    if (request instanceof LocalChannelRequest) {
      LocalChannelRequest localRequest = (LocalChannelRequest) request;
      logger.debug("Added request for {}, queue size now {}", localRequest.processorId, requestQueue.size());
    }
  }

  @Override
  public NetworkRequest receiveRequest() throws InterruptedException {
    NetworkRequest request = requestQueue.take();
    if (request instanceof LocalChannelRequest) {
      LocalChannelRequest localRequest = (LocalChannelRequest) request;
      logger.debug("Removed request for {}, queue size now {}", localRequest.processorId, requestQueue.size());
    }
    return request;
  }

  @Override
  public List<NetworkRequest> getDroppedRequests() {
    return Collections.emptyList();
  }

  @Override
  public void sendResponse(Send payloadToSend, NetworkRequest originalRequest, ServerNetworkResponseMetrics metrics) {
    LocalChannelRequest localRequest = (LocalChannelRequest) originalRequest;
    ResponseInfo responseInfo =
        new ResponseInfo(localRequest.requestInfo, null, localRequest.requestInfo.getReplicaId().getDataNodeId(),
            payloadToSend);
    BatchBlockingQueue<ResponseInfo> responseQueue = getResponseQueue(localRequest.processorId);
    responseQueue.put(responseInfo);
    logger.debug("Added response for {}, size now {}", localRequest.processorId, responseQueue.size());
  }

  /**
   * Receive all queued responses corresponding to requests matching a processor id.
   * @param processorId the processor id to match.
   * @param pollTimeoutMs the poll timeout in msec.
   * @return the applicable responses.
   */
  public List<ResponseInfo> receiveResponses(int processorId, int pollTimeoutMs) {
    List<ResponseInfo> responseList = getResponseQueue(processorId).poll(pollTimeoutMs);
    logger.debug("Returning {} responses for {}", responseList.size(), processorId);
    return responseList;
  }

  /**
   * @return the response list corresponding to a processor id.
   * @param processorId the processor id to match.
   */
  private BatchBlockingQueue<ResponseInfo> getResponseQueue(int processorId) {
    return responseMap.computeIfAbsent(processorId, p -> new BatchBlockingQueue<>(WAKEUP_MARKER));
  }

  @Override
  public void closeConnection(NetworkRequest request) {
  }

  @Override
  public void shutdown() {
  }

  /**
   * Wake up a blocking call to {@link #receiveResponses}. This can be called so that some other entity can do some work
   * on the main event loop even if no responses are received.
   * @param processorId the processor ID to wake up.
   */
  public void wakeup(int processorId) {
    getResponseQueue(processorId).wakeup();
  }

  /**
   * A {@link NetworkRequest} implementation that works with {@link LocalRequestResponseChannel}.
   */
  public static class LocalChannelRequest implements NetworkRequest {
    private RequestInfo requestInfo;
    private long startTimeInMs;
    private int processorId;

    LocalChannelRequest(RequestInfo requestInfo, int processorId) {
      this.requestInfo = requestInfo;
      this.processorId = processorId;
      startTimeInMs = System.currentTimeMillis();
    }

    @Override
    public InputStream getInputStream() {
      return null;
    }

    @Override
    public long getStartTimeInMs() {
      return startTimeInMs;
    }

    @Override
    public boolean release() {
      // release related request content
      return requestInfo.getRequest().release();
    }

    @Override
    public String toString() {
      return "LocalChannelRequest{" + "requestInfo=" + requestInfo + ", startTimeInMs=" + startTimeInMs
          + ", processorId=" + processorId + '}';
    }

    public RequestInfo getRequestInfo() {
      return requestInfo;
    }
  }
}
