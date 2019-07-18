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

import com.github.ambry.config.NetworkConfig;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.Time;
import java.util.ArrayList;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * RequestResponse channel for socket server
 */
public class SocketRequestResponseChannel implements RequestResponseChannel {

  interface ResponseListener {
    void onResponse(int processorId);
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(SocketRequestResponseChannel.class);

  private final int numProcessors;
  private final RequestQueue requestQueue;
  private final ArrayList<BlockingQueue<Response>> responseQueues;
  private final ArrayList<ResponseListener> responseListeners;
  private final Queue<Request> unqueuedRequests = new ConcurrentLinkedQueue<>();

  public SocketRequestResponseChannel(NetworkConfig config) {
    numProcessors = config.numIoThreads;
    Time time = SystemTime.getInstance();
    switch (config.requestQueueType) {
      case ADAPTIVE_LIFO_CO_DEL:
        this.requestQueue =
            new AdaptiveLifoCoDelRequestQueue(config.queuedMaxRequests, 0.7, 5, config.requestQueueTimeoutMs, time);
        break;
      case BASIC_FIFO:
        this.requestQueue = new FifoRequestQueue(config.queuedMaxRequests, config.requestQueueTimeoutMs, time);
        break;
      default:
        throw new IllegalArgumentException("Queue type not supported by channel: " + config.requestQueueType);
    }
    responseQueues = new ArrayList<>(this.numProcessors);
    responseListeners = new ArrayList<>();

    for (int i = 0; i < this.numProcessors; i++) {
      responseQueues.add(i, new LinkedBlockingQueue<>());
    }
  }

  /** Send a request to be handled, potentially blocking until there is room in the queue for the request */
  @Override
  public void sendRequest(Request request) {
    if (!requestQueue.offer(request)) {
      LOGGER.debug("Request queue is full, dropping incoming request: {}", request);
      unqueuedRequests.add(request);
    }
  }

  /** Send a response back to the socket server to be sent over the network */
  @Override
  public void sendResponse(Send payloadToSend, Request originalRequest, ServerNetworkResponseMetrics metrics)
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
  public void closeConnection(Request originalRequest) throws InterruptedException {
    SocketServerResponse response = new SocketServerResponse(originalRequest, null, null);
    responseQueues.get(response.getProcessor()).put(response);
    for (ResponseListener listener : responseListeners) {
      listener.onResponse(response.getProcessor());
    }
  }

  /** Get the next request or block until there is one */
  @Override
  public RequestBundle receiveRequest() throws InterruptedException {
    RequestBundle requestBundle = requestQueue.take();
    Request unqueuedRequest;
    while ((unqueuedRequest = unqueuedRequests.poll()) != null) {
      requestBundle.getRequestsToDrop().add(unqueuedRequest);
    }
    return requestBundle;
  }

  /** Get a response for the given processor if there is one */
  public Response receiveResponse(int processor) {
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
}


