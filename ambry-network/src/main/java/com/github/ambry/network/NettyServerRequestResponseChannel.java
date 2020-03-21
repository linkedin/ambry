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

import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.Callback;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;


/**
 * RequestResponseChannel for Netty based server
 */
public class NettyServerRequestResponseChannel implements RequestResponseChannel {
  private final ArrayBlockingQueue<NetworkRequest> requestQueue;

  public NettyServerRequestResponseChannel(int queueSize) {
    requestQueue = new ArrayBlockingQueue<>(queueSize);
  }

  /** Send a request to be handled, potentially blocking until there is room in the queue for the request */
  @Override
  public void sendRequest(NetworkRequest request) throws InterruptedException {
    DataInputStream stream = new DataInputStream(request.getInputStream());
    try {
      // The first 8 bytes is size of the request. TCP implementation uses this size to allocate buffer. See {@link BoundedReceive}
      // Here we just need to consume it.
      stream.readLong();
    } catch (IOException e) {
      throw new IllegalStateException("stream read error." + e);
    }
    requestQueue.put(request);
  }

  /** Send a response back to the socket server to be sent over the network */
  @Override
  public void sendResponse(Send payloadToSend, NetworkRequest originalRequest, ServerNetworkResponseMetrics metrics)
      throws InterruptedException {

    if (!(originalRequest instanceof NettyServerRequest)) {
      throw new IllegalArgumentException("NetworkRequest should be NettyRequest");
    }

    RestResponseChannel restResponseChannel = ((NettyServerRequest) originalRequest).getRestResponseChannel();
    restResponseChannel.setHeader(RestUtils.Headers.CONTENT_LENGTH, payloadToSend.sizeInBytes());
    payloadToSend.writeTo(restResponseChannel, (result, exception) -> {
    });// an extra copy
  }

  /**
   * Closes the connection and does not send any response
   */
  @Override
  public void closeConnection(NetworkRequest originalRequest) throws InterruptedException {
    throw new UnsupportedOperationException();
  }

  /** Get the next request or block until there is one */
  @Override
  public NetworkRequest receiveRequest() throws InterruptedException {
    return requestQueue.take();
  }

  /**
   * Shuts down the request response channel
   */
  @Override
  public void shutdown() {
    return;
  }
}


