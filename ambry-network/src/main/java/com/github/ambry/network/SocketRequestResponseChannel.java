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
package com.github.ambry.network;

import com.github.ambry.utils.AbstractByteBufHolder;
import com.github.ambry.utils.NettyByteBufDataInputStream;
import com.github.ambry.utils.SystemTime;
import io.netty.buffer.ByteBuf;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


// The request at the network layer
class SocketServerRequest extends AbstractByteBufHolder<SocketServerRequest> implements NetworkRequest {
  private static final Logger logger = LoggerFactory.getLogger(SocketServerRequest.class);
  private final int processor;
  private final String connectionId;
  private final InputStream input;
  private final long startTimeInMs;
  private final ByteBuf content;

  public SocketServerRequest(int processor, String connectionId, ByteBuf content) {
    this.processor = processor;
    this.connectionId = connectionId;
    this.content = content;
    this.input = new NettyByteBufDataInputStream(content);
    this.startTimeInMs = System.currentTimeMillis();
    logger.trace("Processor {} received request : {}", processor, connectionId);
  }

  @Override
  public InputStream getInputStream() {
    return input;
  }

  @Override
  public long getStartTimeInMs() {
    return startTimeInMs;
  }

  public int getProcessor() {
    return processor;
  }

  public String getConnectionId() {
    return connectionId;
  }

  @Override
  public ByteBuf content() {
    return content;
  }

  @Override
  public SocketServerRequest replace(ByteBuf content) {
    return new SocketServerRequest(getProcessor(), getConnectionId(), content);
  }
}

// The response at the network layer
class SocketServerResponse implements NetworkResponse {

  private final int processor;
  private final NetworkRequest request;
  private final Send output;
  private final ServerNetworkResponseMetrics metrics;
  private long startQueueTimeInMs;

  public SocketServerResponse(NetworkRequest request, Send output, ServerNetworkResponseMetrics metrics) {
    this.request = request;
    this.output = output;
    this.processor = ((SocketServerRequest) request).getProcessor();
    this.metrics = metrics;
  }

  public Send getPayload() {
    return output;
  }

  public NetworkRequest getRequest() {
    return request;
  }

  public int getProcessor() {
    return processor;
  }

  public void onEnqueueIntoResponseQueue() {
    this.startQueueTimeInMs = System.currentTimeMillis();
  }

  public void onDequeueFromResponseQueue() {
    if (metrics != null) {
      metrics.updateQueueTime(System.currentTimeMillis() - startQueueTimeInMs);
    }
  }

  public ServerNetworkResponseMetrics getMetrics() {
    return metrics;
  }
}

interface ResponseListener {
  void onResponse(int processorId);
}

/**
 * RequestResponse channel for socket server
 */
public class SocketRequestResponseChannel implements RequestResponseChannel {
  private final int numProcessors;
  private final int queueSize;
  private final ArrayBlockingQueue<NetworkRequest> requestQueue;
  private final ArrayList<BlockingQueue<NetworkResponse>> responseQueues;
  private final ArrayList<ResponseListener> responseListeners;

  public SocketRequestResponseChannel(int numProcessors, int queueSize) {
    this.numProcessors = numProcessors;
    this.queueSize = queueSize;
    this.requestQueue = new ArrayBlockingQueue<>(this.queueSize);
    responseQueues = new ArrayList<>(this.numProcessors);
    responseListeners = new ArrayList<ResponseListener>();

    for (int i = 0; i < this.numProcessors; i++) {
      responseQueues.add(i, new LinkedBlockingQueue<>());
    }
  }

  /** Send a request to be handled, potentially blocking until there is room in the queue for the request */
  @Override
  public void sendRequest(NetworkRequest request) throws InterruptedException {
    requestQueue.put(request);
  }

  /** Send a response back to the socket server to be sent over the network */
  @Override
  public void sendResponse(Send payloadToSend, NetworkRequest originalRequest, ServerNetworkResponseMetrics metrics)
      throws InterruptedException {
    SocketServerResponse response = new SocketServerResponse(originalRequest, payloadToSend, metrics);
    response.onEnqueueIntoResponseQueue();
    responseQueues.get(response.getProcessor()).put(response);
    for (ResponseListener listener : responseListeners) {
      listener.onResponse(response.getProcessor());
    }
  }

  /**
   * Closes the connection and does not send any response
   */
  @Override
  public void closeConnection(NetworkRequest originalRequest) throws InterruptedException {
    SocketServerResponse response = new SocketServerResponse(originalRequest, null, null);
    responseQueues.get(response.getProcessor()).put(response);
    for (ResponseListener listener : responseListeners) {
      listener.onResponse(response.getProcessor());
    }
  }

  /** Get the next request or block until there is one */
  @Override
  public NetworkRequest receiveRequest() throws InterruptedException {
    return requestQueue.take();
  }

  /** Get a response for the given processor if there is one */
  public NetworkResponse receiveResponse(int processor) throws InterruptedException {
    return responseQueues.get(processor).poll();
  }

  public void addResponseListener(ResponseListener listener) {
    responseListeners.add(listener);
  }

  public int getRequestQueueSize() {
    return requestQueue.size();
  }

  public int getResponseQueueSize(int processor) {
    return responseQueues.get(processor).size();
  }

  public int getNumberOfProcessors() {
    return numProcessors;
  }

  public void shutdown() {
    requestQueue.forEach(NetworkRequest::release);
    requestQueue.clear();
  }
}


