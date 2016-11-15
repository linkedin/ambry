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

import com.github.ambry.utils.SystemTime;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


// The request at the network layer
class SocketServerRequest implements Request {
  private final int processor;
  private final String connectionId;
  private final InputStream input;
  private final long startTimeInMs;
  private Logger logger = LoggerFactory.getLogger(getClass());

  public SocketServerRequest(int processor, String connectionId, InputStream input) throws IOException {
    this.processor = processor;
    this.connectionId = connectionId;
    this.input = input;
    this.startTimeInMs = SystemTime.getInstance().milliseconds();
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
}

// The response at the network layer
class SocketServerResponse implements Response {

  private final int processor;
  private final Request request;
  private final Send output;
  private final ServerNetworkResponseMetrics metrics;
  private long startQueueTimeInMs;

  public SocketServerResponse(Request request, Send output, ServerNetworkResponseMetrics metrics) {
    this.request = request;
    this.output = output;
    this.processor = ((SocketServerRequest) request).getProcessor();
    this.metrics = metrics;
  }

  public Send getPayload() {
    return output;
  }

  public Request getRequest() {
    return request;
  }

  public int getProcessor() {
    return processor;
  }

  public void onEnqueueIntoResponseQueue() {
    this.startQueueTimeInMs = SystemTime.getInstance().milliseconds();
  }

  public void onDequeueFromResponseQueue() {
    if (metrics != null) {
      metrics.updateQueueTime(SystemTime.getInstance().milliseconds() - startQueueTimeInMs);
    }
  }

  public NetworkSendMetrics getMetrics() {
    return metrics;
  }
}

interface ResponseListener {
  public void onResponse(int processorId);
}

/**
 * RequestResponse channel for socket server
 */
public class SocketRequestResponseChannel implements RequestResponseChannel {
  private final int numProcessors;
  private final int queueSize;
  private final ArrayBlockingQueue<Request> requestQueue;
  private final ArrayList<BlockingQueue<Response>> responseQueues;
  private final ArrayList<ResponseListener> responseListeners;

  public SocketRequestResponseChannel(int numProcessors, int queueSize) {
    this.numProcessors = numProcessors;
    this.queueSize = queueSize;
    this.requestQueue = new ArrayBlockingQueue<Request>(this.queueSize);
    responseQueues = new ArrayList<BlockingQueue<Response>>(this.numProcessors);
    responseListeners = new ArrayList<ResponseListener>();

    for (int i = 0; i < this.numProcessors; i++) {
      responseQueues.add(i, new LinkedBlockingQueue<Response>());
    }
  }

  /** Send a request to be handled, potentially blocking until there is room in the queue for the request */
  @Override
  public void sendRequest(Request request) throws InterruptedException {
    requestQueue.put(request);
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
  public Request receiveRequest() throws InterruptedException {
    return requestQueue.take();
  }

  /** Get a response for the given processor if there is one */
  public Response receiveResponse(int processor) throws InterruptedException {
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
    requestQueue.clear();
  }
}


